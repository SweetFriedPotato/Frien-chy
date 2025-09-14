import csv
from qdrant_client import QdrantClient

QDRANT_HOST = "128.199.94.121"
QDRANT_API_KEY = "ucarjfdljldjljereu"
COLLECTION_NAME = "franchise-db-1"
QDRANT_URL = f"http://{QDRANT_HOST}:6333"

client = QdrantClient(url=QDRANT_URL, api_key=QDRANT_API_KEY)

print(f"🔍 '{COLLECTION_NAME}' 컬렉션에서 모든 브랜드 정보 추출을 시작합니다...")
print("(데이터 양에 따라 몇 분 정도 소요될 수 있습니다.)")

brands = set() # 중복 저장을 방지하기 위해 set을 사용
next_offset = None

try:
    while True:
        points, next_offset = client.scroll(
            collection_name=COLLECTION_NAME,
            limit=250,
            offset=next_offset,
            with_payload=["metadata"]
        )

        for point in points:
            metadata = point.payload.get('metadata', {})
            brand_name = metadata.get('brand_name')

            if brand_name:
                brands.add(brand_name)

        # 진행 상황 표시
        print(f"  -> 현재까지 찾은 고유 브랜드 수: {len(brands)}개", end='\r')

        if next_offset is None:
            break

except Exception as e:
    print(f"\n❌ 오류 발생: {e}")
    exit()

print("\n")

# CSV 파일로 저장
output_filename = 'brand_list_from_qdrant.csv'
with open(output_filename, 'w', encoding='utf-8', newline='') as f:
    writer = csv.writer(f)
    # --- ★★★ 업종(industry)은 비워두도록 수정 ★★★ ---
    writer.writerow(['official_name', 'industry', 'aliases'])
    for brand_name in sorted(list(brands)):
        # official_name에는 추출한 브랜드 이름을 넣고, industry와 aliases는 비워둡니다.
        writer.writerow([brand_name, '', ''])

print(f"🎉 추출 완료! 총 {len(brands)}개의 고유한 브랜드 정보를 '{output_filename}' 파일에 저장했습니다.")
print("이제 이 CSV 파일을 열어서 'industry'와 'aliases' 컬럼을 채워주세요!")
