# Ruff 가이드

## Ruff란?

Python 코드를 검사(린트)하고 스타일 통일(포맷)하는 도구,
팀 전체가 동일한 규칙으로 코드를 작성할 수 있게 해줍니다.

### 린터 vs 포매터

| | 린터 (Linter) | 포매터 (Formatter) |
|---|------|--------|
| 역할 | 버그/규칙 위반 **검사** | 코드 스타일 **통일** |
| 비유 | 맞춤법 검사기 | 자동 줄 맞춤 |
| 예시 | 안 쓰는 import, 정의 안 된 변수, import 순서 | 공백, 들여쓰기, 따옴표, 줄바꿈 |

---

## 기본 사용법

### 린트 (코드 검사)

```bash
# 문제 찾기
ruff check .

# 자동으로 고칠 수 있는 것들 수정
ruff check --fix .
```

### 포맷 (스타일 통일)

```bash
# 포맷이 안 맞는 파일 확인만 (수정 X)
ruff format --check .

# 실제로 파일 수정
ruff format .
```

### 특정 파일/폴더만 실행

```bash
ruff check data_pipeline/load/
ruff format dashboard/
```

---

## 커밋 전 루틴

```bash
ruff check --fix .   # 린트 자동 수정
ruff format .        # 포맷 정리
git add .
git commit -m "..."
```


