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
# python3 data_pipeline/extract/download_gov24_land.py (parquet 파일 상위 폴더) [시작 인덱스 (기본값 0)] [끝 인덱스 + 1 (기본값 마지막 row)] [pdf 저장 경로]
$ python3 data_pipeline/extract/download_gov24_land.py ./data/output/silver_stage_1/byproduct/region=경기/sigungu=41461/year=2025 --start 10 --end 20
```


### 토지대장 pdf 파싱 -> CSV 파일 변환 (주소, 지번, 지주)
```bash
# python3 parsing_pdf.py pdf_folder
$ python3 data_pipeline/extract/parsing_pdf.py ./data/tojidaejang/_work/output
```