import os
import re
import csv
from pathlib import Path
from multiprocessing import Pool, cpu_count

from pdf2image import convert_from_path
import pytesseract
from tqdm import tqdm

# =========================
# 설정
# =========================
PDF_ROOT = "pdf"
OUTPUT_CSV = "land_owners.csv"

# =========================
# OCR + crop + 이름 추출
# =========================
def extract_owner_from_crop(img, address, lot_num, page_idx):
    finish_flag = False

    img = img.rotate(90, expand=True)

    w, h = img.size

    candidates = []

    # 슬라이딩 시작 위치
    y = 0.335

    while y <= 0.515:

        crop = img.crop((
            int(w * 0.598),
            int(h * y),
            int(w * 0.827),
            int(h * (y + 0.025))
        ))

        # crop = crop.resize((crop.width*2, crop.height*2))

        # tmp 저장 (디버그용)
        # tmp_path = Path("./tmp") / f"tmp_{address}_{lot_num}_p{page_idx}_{y:.3f}.png"
        # crop.save(tmp_path)

        text = pytesseract.image_to_string(
            crop,
            lang="kor",
            config="--psm 6"
        )

        for line in text.split("\n"):

            line = line.strip()

            if not line:
                continue

            # "외 N인" 처리
            # 예: 홍길동 외 2인 → 홍길동 추출
            match = re.search(r"([가-힣]+)\s*외\s*\d+\s*인", line)

            if match:
                candidates.append(match.group(1))
                continue

            # 공백 있으면 후보 아님
            if " " in line:
                continue

            # 한글로 시작하지 않으면 후보 아님
            if not re.match(r"^[가-힣]", line):
                continue

            # 특수문자 있으면 후보 아님
            if re.search(r"[^가-힣]", line):
                continue
            
            candidates.append(line)

        y += 0.06

    return (candidates[-1], finish_flag) if candidates else ("", finish_flag)


# =========================
# PDF 처리
# =========================
def process_pdf(pdf_path):
    try:

        filename = Path(pdf_path).stem
        parts = filename.split("_")

        # base 주소 (지번 제외)
        base_addr = " ".join(parts[:-1])

        # 지번
        lot_number = parts[-1]

        images = convert_from_path(pdf_path, dpi=250)

        latest_owner = ""

        # 뒤에서부터 검사
        for i in range(len(images)):

            owner, finish_flag = extract_owner_from_crop(
                images[i],
                base_addr,
                lot_number,
                i + 1
            )

            if owner:
                latest_owner = owner
            
            if finish_flag:
                break

        return (base_addr, lot_number, latest_owner)

    except Exception as e:

        print("❌ 실패:", pdf_path, e)

        filename = Path(pdf_path).stem
        parts = filename.split("_")

        return (
            " ".join(parts[:-1]),
            parts[-1],
            ""
        )

# =========================
# 메인 실행
# =========================
def main():

    pdf_files = list(Path(PDF_ROOT).rglob("*.pdf"))

    print(f"\n📄 PDF 발견: {len(pdf_files)}개")

    workers = max(cpu_count() - 1, 1)

    with Pool(workers) as pool:

        results = list(tqdm(
            pool.imap(process_pdf, pdf_files),
            total=len(pdf_files)
        ))

    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8-sig") as f:

        writer = csv.writer(f)
        writer.writerow(["주소", "지번", "지주"])
        writer.writerows(results)

    print("\n✅ CSV 저장 완료:", OUTPUT_CSV)


# =========================
# 시작
# =========================
if __name__ == "__main__":
    main()
