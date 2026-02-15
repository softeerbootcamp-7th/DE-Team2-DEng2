# Install
```bash
$ pip install selenium pdf2image pytesseract pillow tqdm
```
```bash
$ brew install tesseract tesseract-lang poppler
```

# Execute
### 정부24 토지대장 다운로드
```bash
# python3 <프로그램 경로> <parquet 파일 상위 폴더> [시작 인덱스 (기본값 0)] [끝 인덱스 + 1 (기본값 row 수)] [pdf 저장 경로]
$ python3 data_pipeline/extract/extract_gov24_land.py ./data/output/silver_stage_1/byproduct/region=경기/sigungu=41461/year=2025 --start 10 --end 20
```

* 실패한 인덱스는 프로그램 종료 후 <pdf 저장 경로>/logs/failures.csv로 저장됨
* 실패한 인덱스만 모은 parquet 파일도 생성되니 이후 아래 명령어로 실패한 인덱스만 크롤링 가능

```bash
$ python3 data_pipeline/extract/extract_gov24_land.py ./data/tojidaejang/_work/output/logs
```


### 토지대장 pdf 파싱 -> CSV 파일 변환 (주소, 지번, 지주)
```bash
# python3 extract_owner_from_pdf.py pdf_folder
$ python3 data_pipeline/extract/extract_owner_from_pdf.py ./data/tojidaejang/_work/output
```