import json
from qdrant_client import QdrantClient

# --- 접속 정보는 이전과 동일합니다 ---
QDRANT_HOST = "128.199.94.121"
QDRANT_API_KEY = "ucarjfdljldjljereu"
COLLECTION_NAME = "franchise-db-1"
QDRANT_URL = f"http://{QDRANT_HOST}:6333"

client = QdrantClient(url=QDRANT_URL, api_key=QDRANT_API_KEY)

print(f"🕵️ '{COLLECTION_NAME}' 컬렉션에서 데이터 1개를 가져와 구조를 확인합니다...")

try:
    # --- ★★★ 여기가 핵심 ★★★ ---
    # limit=1 로 설정하여 딱 1개의 데이터 포인트만 가져옵니다.
    points, _ = client.scroll(
        collection_name=COLLECTION_NAME,
        limit=1,
        with_payload=True  # payload 전체를 다 가져옵니다.
    )

    if points:
        # 가져온 첫 번째 데이터의 payload(실제 내용물)를 예쁘게 출력합니다.
        print("\n✅ 데이터 1개 가져오기 성공! payload 내용은 아래와 같습니다:\n")
        payload_content = points[0].payload
        print(json.dumps(payload_content, indent=2, ensure_ascii=False))
        print("\n--------------------------------------------------")
        print("위 내용에서 '브랜드 이름'과 '업종'에 해당하는 정확한 키(key) 이름을 확인하세요.")
        print("예: 'brand_nm', 'indus_nm' 등")
        print("--------------------------------------------------")
    else:
        print("\n❌ 컬렉션에 데이터가 없거나, 접근에 실패했습니다.")

except Exception as e:
    print(f"\n❌ 오류 발생: {e}")
