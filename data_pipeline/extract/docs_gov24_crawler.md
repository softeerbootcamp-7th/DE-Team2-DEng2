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
$ python3 download_gov24_land.py filtered.csv 100 150
```

### 토지대장 pdf 파싱 -> CSV 파일 변환 (주소, 지번, 지주)
```bash
$ python3 parsing_pdf.py 
```