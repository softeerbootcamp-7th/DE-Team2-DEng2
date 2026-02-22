import os
import re
import csv
import sys
from pathlib import Path
from datetime import datetime
from multiprocessing import Pool, cpu_count

import pandas as pd
from pdf2image import convert_from_path
import pytesseract
from tqdm import tqdm


# =========================
# 지번 분리
# =========================
def split_lot_number(lot: str):
    lot = lot.strip()

    if "-" in lot:
        main, sub = lot.split("-", 1)
        return main.strip(), sub.strip()

    return lot.strip(), None  # ✅ 부번 없음 → NULL


# =========================
# OCR + crop + 이름 추출
# =========================
def extract_owner_from_crop(img, address, lot_num, page_idx):

    img = img.rotate(90, expand=True)

    w, h = img.size
    candidates = []

    y = 0.335

    while y <= 0.515:

        crop = img.crop((
            int(w * 0.598),
            int(h * y),
            int(w * 0.827),
            int(h * (y + 0.025))
        ))

        text = pytesseract.image_to_string(
            crop,
            lang="kor",
            config="--psm 6"
        )

        for line in text.split("\n"):

            line = line.strip()
            if not line:
                continue

            match = re.search(r"([가-힣]+)\s*외\s*\d+\s*인", line)
            if match:
                candidates.append(match.group(1))
                continue

            if " " in line:
                continue

            if not re.match(r"^[가-힣]", line):
                continue

            if re.search(r"[^가-힣]", line):
                continue

            candidates.append(line)

        y += 0.06

    return candidates[-1] if candidates else ""


# =========================
# PDF 처리
# =========================
def process_pdf(pdf_path):

    try:

        filename = Path(pdf_path).stem
        parts = filename.split("_")

        if len(parts) < 3:
            print("⚠ 파일명 형식 오류:", filename)
            return None

        # ✅ 주소 / 지번 / 날짜 파싱
        base_addr = " ".join(parts[:-2])
        lot_number = parts[-2]
        changed_date = parts[-1]

        main_no, sub_no = split_lot_number(lot_number)

        images = convert_from_path(pdf_path, dpi=250)

        latest_owner = ""

        for i, img in enumerate(images):

            owner = extract_owner_from_crop(
                img,
                base_addr,
                lot_number,
                i + 1
            )

            if owner:
                latest_owner = owner

        # 지주 없으면 제외
        if not latest_owner:
            return None

        return (
            base_addr,
            main_no,
            sub_no,
            latest_owner,
            changed_date
        )

    except Exception as e:

        print("❌ 실패:", pdf_path, e)
        return None


# =========================
# 메인 실행
# =========================
def main():

    if len(sys.argv) < 2:
        print("사용법: python parsing_pdf.py <pdf_folder>")
        sys.exit(1)

    pdf_root = Path(sys.argv[1]).resolve()

    if not pdf_root.exists():
        print("❌ pdf 폴더 경로가 존재하지 않음:", pdf_root)
        sys.exit(1)

    # 프로젝트 루트: extract_owner_from_pdf.py → extract/ → data_pipeline/ → 프로젝트 루트
    project_root = Path(__file__).resolve().parent.parent.parent

    output_dir = project_root / "data" / "silver" / "s2" / "ownership_inference"
    output_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_path = output_dir / f"ocr_result_{timestamp}.csv"
    parquet_path = output_dir / f"ocr_result_{timestamp}.parquet"

    pdf_files = list(pdf_root.rglob("*.pdf"))

    print(f"\n📄 PDF 발견: {len(pdf_files)}개")

    workers = max(cpu_count() - 1, 1)

    with Pool(workers) as pool:

        results = list(filter(
            None,
            tqdm(
                pool.imap(process_pdf, pdf_files),
                total=len(pdf_files)
            )
        ))

    if not results:
        return

    # ✅ DataFrame 생성
    df = pd.DataFrame(
        results,
        columns=[
            "주소",
            "본번",
            "부번",
            "지주",
            "소유권변동일자"
        ]
    )

    # 부번 NULL 유지 (pandas nullable 타입)
    df["부번"] = df["부번"].astype("object")

    # 저장
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    df.to_parquet(parquet_path, index=False)

    print("\n✅ 저장 완료:")
    print("CSV:", csv_path)
    print("Parquet:", parquet_path)


# =========================
# 시작
# =========================
if __name__ == "__main__":
    main()
