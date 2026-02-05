import requests
import xml.etree.ElementTree as ET
import xmltodict
import pymongo
from pymongo import MongoClient
from datetime import datetime
import time
import os

# GitHub Secrets 로드
MONGO_URI = os.environ.get('MONGO_URI')
BASE44_KEY = os.environ.get('BASE44_KEY')
EKAPE_KEY = os.environ.get('EKAPE_KEY')
DEFAULT_MTRACE_ID = os.environ.get('MTRACE_ID')
DEFAULT_MTRACE_KEY = os.environ.get('MTRACE_KEY')
CNU_MTRACE_ID = os.environ.get('CNU_MTRACE_ID')
CNU_MTRACE_KEY = os.environ.get('CNU_MTRACE_KEY')

# DB 연결
client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000) # 타임아웃 5초 설정
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
        print(f"   ❌ API 요청 실패: {url[:30]}... 에러: {e}")
        return None

def run_integrated_pipeline():
    # 1. Base44 농장 정보 업데이트
    print("1️⃣ [Base44] 농장 정보 동기화 중...")
    res_b44 = safe_request("https://app.base44.com/api/apps/68ccb7f3c0a6ef99bbf4ad23/entities/Farm")
    if res_b44:
        farms_data = res_b44.json()
        print(f"   -> {len(farms_data)}개의 농장 발견.")
        for farm in farms_data:
            db['FarmInfo'].update_one({"farm_unique_no": farm['farm_unique_no']}, {"$set": farm}, upsert=True)

    # 2. 개체 수집 및 상태 업데이트
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

        print(f"🚜 [{f_name}] 개체 리스트 수집 시작...")
        p_brd = {"userId": curr_id, "apiKey": curr_key, "farmUniqueNo": f_id, "farmerNm": f_owner, "farmerHtelNo": f_phone}
        res_brd = safe_request("https://api.mtrace.go.kr/rest/myFarmData/farmUniqNoCattleBrdIndvd", json_data=p_brd, method='POST')
        
        if res_brd:
            try:
                rows = ET.fromstring(res_brd.content).findall(".//row")
                print(f"   -> {len(rows)}두의 개체 확인됨.")
                for row in rows:
                    c_no = row.find('animalNo').text
                    db['AnimalMaster'].update_one({"cattleNo": c_no}, {"$set": {"farm_id": f_id, "status": "사육", "last_updated": now}}, upsert=True)
            except Exception as e: 
                print(f"   ❌ 파싱 에러: {e}")

    # 3. 상세 정보 수집 (테스트용: 10마리 제한)
    all_animals = list(db['AnimalMaster'].find().limit(10)) # <--- ⚠️ 10마리 제한!!
    print(f"2️⃣ [테스트] 총 {len(all_animals)}두 상세 정보 수집 시작...")

    for idx, animal in enumerate(all_animals):
        a_no = animal['cattleNo']
        print(f"   🔍 ({idx+1}/{len(all_animals)}) {a_no} 상세 정보 가져오는 중...")
        
        history_bundle = {}
        # 1~9번 옵션 중 핵심인 1, 2, 6번만 우선 수집 (테스트 속도 향상)
        for opt in [1, 2, 6]: 
            res_h = safe_request("http://data.ekape.or.kr/openapi-data/service/user/animalTrace/traceNoSearch",
                                 params={"ServiceKey": EKAPE_KEY, "traceNo": a_no, "optionNo": opt})
            if res_h:
                try:
                    history_bundle[f"opt_{opt}"] = xmltodict.parse(res_h.text)
                except: pass
            time.sleep(0.1) # 서버 부하 방지
        
        db['AnimalHistoryDetail'].update_one(
            {"cattleNo": a_no},
            {"$set": {"history": history_bundle, "status": animal.get('status'), "last_updated": now}},
            upsert=True
        )

    print("✅ 테스트 공정 완료.")

if __name__ == "__main__":
    run_integrated_pipeline()
