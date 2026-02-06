import requests
import xml.etree.ElementTree as ET
import xmltodict
import pymongo
from pymongo import MongoClient
from datetime import datetime, timedelta
import time
import os

# 1. 환경 변수 설정 (GitHub Secrets)
MONGO_URI = os.environ.get('MONGO_URI')
BASE44_KEY = os.environ.get('BASE44_KEY')
EKAPE_KEY = os.environ.get('EKAPE_KEY')
DEFAULT_MTRACE_ID = os.environ.get('MTRACE_ID')
DEFAULT_MTRACE_KEY = os.environ.get('MTRACE_KEY')
CNU_MTRACE_ID = os.environ.get('CNU_MTRACE_ID')
CNU_MTRACE_KEY = os.environ.get('CNU_MTRACE_KEY')

# 2. DB 연결 및 설정
client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
db = client['Livestock_Data_Hub']
now = datetime.now()

def safe_request(url, params=None, json_data=None, method='GET'):
    try:
        headers = {'api_key': BASE44_KEY, 'Content-Type': 'application/json'} if 'base44' in url else {}
        if method == 'GET':
            res = requests.get(url, params=params, headers=headers, timeout=10)
        else:
            res = requests.post(url, json=json_data, timeout=10)
        res.raise_for_status()
        return res
    except Exception as e:
        print(f"   ❌ API 실패: {url[:30]}... ({e})")
        return None

def run_integrated_pipeline():
    # --- [STEP 1] 농장 정보 동기화 ---
    print("1️⃣ [Base44] 농장 정보 동기화 중...")
    res_b44 = safe_request("https://app.base44.com/api/apps/68ccb7f3c0a6ef99bbf4ad23/entities/Farm")
    if res_b44:
        farms_data = res_b44.json()
        for farm in farms_data:
            db['FarmInfo'].update_one({"farm_unique_no": farm['farm_unique_no']}, {"$set": farm}, upsert=True)

    # --- [STEP 2] 개체 마스터 목록 업데이트 ---
    farms = list(db['FarmInfo'].find({}))
    for farm in farms:
        f_name = farm.get('farm_name', 'Unknown')
        f_id_raw = farm.get('farm_unique_no', '')
        f_id = str(f_id_raw[0] if isinstance(f_id_raw, list) else f_id_raw).replace('-', '').strip()
        f_owner = str(farm.get('owner_name', '')).strip()
        f_phone_raw = farm.get('phone', '')
        f_phone = str(f_phone_raw[0] if isinstance(f_phone_raw, list) else f_phone_raw).replace('-', '').strip()

        if not f_id or not f_owner or not f_phone: continue

        is_cnu = "충남대학교" in f_name
        curr_id = CNU_MTRACE_ID if is_cnu else DEFAULT_MTRACE_ID
        curr_key = CNU_MTRACE_KEY if is_cnu else DEFAULT_MTRACE_KEY

        print(f"🚜 [{f_name}] 개체 리스트 동기화...")
        p_brd = {"userId": curr_id, "apiKey": curr_key, "farmUniqueNo": f_id, "farmerNm": f_owner, "farmerHtelNo": f_phone}
        res_brd = safe_request("https://api.mtrace.go.kr/rest/myFarmData/farmUniqNoCattleBrdIndvd", json_data=p_brd, method='POST')
        
        if res_brd:
            try:
                rows = ET.fromstring(res_brd.content).findall(".//row")
                for row in rows:
                    c_no = row.find('animalNo').text
                    # 마스터 정보 저장 시 status만 기록 (상세 수집 대상 선별용)
                    db['AnimalMaster'].update_one(
                        {"cattleNo": c_no}, 
                        {"$set": {"farm_id": f_id, "status": "사육"}}, 
                        upsert=True
                    )
            except: pass

    # --- [STEP 3] 상세 정보 선택적 수집 (핵심 로직) ---
    # 24시간 이내에 업데이트된 적이 없는 소들만 골라냅니다.
    one_day_ago = datetime.now() - timedelta(days=1)
    
    query = {
        "$or": [
            {"last_updated": {"$lt": one_day_ago}},      # 업데이트된 지 24시간이 넘었거나
            {"last_updated": {"$exists": False}}         # 한 번도 업데이트된 적 없는 소
        ]
    }
    
    # 오래된 순서대로 500두만 가져옵니다.
    all_animals = list(db['AnimalMaster'].find(query).sort("last_updated", 1).limit(500))
    
    print(f"2️⃣ [대상 선정] 업데이트가 필요한 소 {len(all_animals)}두 수집 시작...")

    for idx, animal in enumerate(all_animals):
        a_no = animal['cattleNo']
        print(f"   🔍 ({idx+1}/{len(all_animals)}) {a_no} 상세 이력 수집 중...")
        
        history_bundle = {}
        # 1~9번 옵션 전체 수집
        for opt in range(1, 10): 
            res_h = safe_request("http://data.ekape.or.kr/openapi-data/service/user/animalTrace/traceNoSearch",
                                 params={"ServiceKey": EKAPE_KEY, "traceNo": a_no, "optionNo": opt})
            if res_h:
                try:
                    history_bundle[f"opt_{opt}"] = xmltodict.parse(res_h.text)
                except: pass
            time.sleep(0.05) 
        
        # 상세 데이터 저장
        db['AnimalHistoryDetail'].update_one(
            {"cattleNo": a_no},
            {"$set": {
                "history": history_bundle, 
                "status": animal.get('status'), 
                "last_updated": datetime.now() 
            }},
            upsert=True
        )
        
        # 중요: AnimalMaster에도 업데이트 시간을 기록하여 다음 실행 때 중복되지 않게 함
        db['AnimalMaster'].update_one(
            {"cattleNo": a_no}, 
            {"$set": {"last_updated": datetime.now()}} 
        )

    print(f"✅ 이번 배치(최대 500두) 공정 완료.")

if __name__ == "__main__":
    run_integrated_pipeline()

