import re
import datetime as dt
import pandas as pd
from pathlib import Path

# 경로
SRC_DIR = Path("./data/bronze/coord/_work/unzipped")
DST_BASE = Path("./data/bronze/coord/parquet")
DST_BASE.mkdir(parents=True, exist_ok=True)

# =========================================================
# year / month (현재 기준)
# =========================================================
today = dt.date.today()

# 이번 달 1일
first_day_this_month = today.replace(day=1)

# 직전 달 마지막 날
last_day_prev_month = first_day_this_month - dt.timedelta(days=1)

YEAR = last_day_prev_month.year
MONTH = f"{last_day_prev_month.month:02d}"

# =========================================================
# region 영어 → 한글 매핑
# =========================================================
REGION_MAP = {
    "seoul": "서울",
    "busan": "부산",
    "daegu": "대구",
    "incheon": "인천",
    "gwangju": "광주",
    "daejeon": "대전",
    "ulsan": "울산",
    "sejong": "세종",
    "gyunggi": "경기",
    "gangwon": "강원",
    "chungbuk": "충북",
    "chungnam": "충남",
    "jeonbuk": "전북",
    "jeonnam": "전남",
    "gyeongbuk": "경북",
    "gyeongnam": "경남",
    "jeju": "제주",
}

# =========================================================
# 컬럼
# =========================================================
FULL_COLUMNS = [
    "시군구코드",
    "출입구일련번호",
    "법정동코드",
    "시도명",
    "시군구명",
    "읍면동명",
    "도로명코드",
    "도로명",
    "지하여부",
    "건물본번",
    "건물부번",
    "건물명",
    "우편번호",
    "건물용도분류",
    "건물군여부",
    "관할행정동",
    "X좌표",
    "Y좌표"
]

# =========================================================
# TXT 안전 로딩
# =========================================================
def read_korean_txt(path: Path) -> pd.DataFrame:
    encodings = ["cp949", "euc-kr", "utf-8"]
    for enc in encodings:
        try:
            return pd.read_csv(
                path,
                sep="|",
                header=None,
                encoding=enc,
                dtype="string",
                keep_default_na=False,
                na_filter=False,
                low_memory=False,
            )
        except UnicodeDecodeError:
            continue
    raise ValueError(f"인코딩 실패: {path}")

# =========================================================
# 파일명 → region 추출
# rnaddrkor_gyunggi.txt
# =========================================================
def extract_region_from_filename(name: str) -> str:
    m = re.match(r"entrc_(.+)\.txt$", name.lower())
    if not m:
        raise ValueError(f"파일명 형식 이상: {name}")

    region_eng = m.group(1)

    if region_eng not in REGION_MAP:
        raise ValueError(f"region 매핑 없음: {region_eng}")

    return REGION_MAP[region_eng]

# =========================================================
# 변환 시작
# =========================================================
txt_files = sorted(SRC_DIR.glob("*.txt"))
if not txt_files:
    raise FileNotFoundError(f"TXT 없음: {SRC_DIR}")

for txt_path in txt_files:

    print("읽는 중:", txt_path.name)

    # region 추출
    region_kor = extract_region_from_filename(txt_path.name)

    # partition 폴더 생성
    dst_dir = (
        DST_BASE
        / f"year={YEAR}"
        / f"month={MONTH}"
        / f"region={region_kor}"
    )
    dst_dir.mkdir(parents=True, exist_ok=True)

    # 데이터 읽기
    df = read_korean_txt(txt_path)

    if df.shape[1] != len(FULL_COLUMNS):
        raise ValueError(
            f"{txt_path.name} 컬럼 개수 불일치: "
            f"{df.shape[1]} vs {len(FULL_COLUMNS)}"
        )

    df.columns = FULL_COLUMNS

    # parquet 저장 (파일 여러개 가능 → part 방식)
    out_path = dst_dir / f"part-{txt_path.stem}.parquet"
    df.to_parquet(out_path, index=False, engine="pyarrow", compression="snappy")

    print("저장:", out_path)

print("전체 변환 완료 ✅")
