import pandas as pd
from pathlib import Path

# 경로
SRC_DIR = Path("./data/address/_work")
DST_DIR = Path("./data/address/parquet")

DST_DIR.mkdir(parents=True, exist_ok=True)

# =========================================================
# 컬럼 정의 (jibun 아닌 경우만)
# =========================================================

FULL_COLUMNS = [
    "도로명주소관리번호",
    "법정동코드",
    "시도명",
    "시군구명",
    "법정읍면동명",
    "법정리명",
    "산여부",
    "지번본번(번지)",
    "지번부번(호)",
    "도로명코드",
    "도로명",
    "지하여부",
    "건물본번",
    "건물부번",
    "행정동코드",
    "행정동명",
    "기초구역번호(우편번호)",
    "이전도로명주소",
    "효력발생일",
    "공동주택구분",
    "이동사유코드",
    "건축물대장건물명",
    "시군구용건물명",
    "비고",
]

# =========================================================
# UTIL
# =========================================================

def read_korean_txt(path):
    """한국 공공데이터 txt 안전하게 읽기"""
    encodings = ["cp949", "euc-kr", "utf-8"]

    for enc in encodings:
        try:
            return pd.read_csv(
                path,
                sep="|",
                header=None,
                encoding=enc
            )
        except UnicodeDecodeError:
            continue

    raise ValueError(f"인코딩 실패: {path}")

# =========================================================
# 전체 변환
# =========================================================

for txt_path in SRC_DIR.glob("*.txt"):
    print("읽는 중:", txt_path.name)

    df = read_korean_txt(txt_path)

    # ⭐ jibun으로 시작하지 않는 파일만 컬럼 지정
    if not txt_path.name.startswith("jibun"):
        if df.shape[1] != len(FULL_COLUMNS):
            raise ValueError(
                f"{txt_path.name} 컬럼 개수 불일치: "
                f"{df.shape[1]} vs {len(FULL_COLUMNS)}"
            )
        df.columns = FULL_COLUMNS

        out_path = DST_DIR / (txt_path.stem + ".parquet")
        df.to_parquet(out_path, index=False)

        print("저장 완료:", out_path)

print("🎉 전체 변환 완료")
