import requests
import xml.etree.ElementTree as ET
import xmltodict
import pymongo
from pymongo import MongoClient
from datetime import datetime
import time
import os

# GitHub Secrets에서 불러올 정보들
MONGO_URI = os.environ.get('MONGO_URI')
BASE44_KEY = os.environ.get('BASE44_KEY')
EKAPE_KEY = os.environ.get('EKAPE_KEY')
MTRACE_ID = os.environ.get('MTRACE_ID')
MTRACE_KEY = os.environ.get('MTRACE_KEY')
CNU_MTRACE_ID = os.environ.get('CNU_MTRACE_ID')
CNU_MTRACE_KEY = os.environ.get('CNU_MTRACE_KEY')

# --- [설정 정보] ---

client = MongoClient(MONGO_URI)
db = client['Livestock_Data_Hub']
now = datetime.now()

def safe_request(url, params=None, json_data=None, method='GET'):
    try:
        headers = {'api_key': BASE44_KEY, 'Content-Type': 'application/json'} if 'base44' in url else {}
        if method == 'GET':
            res = requests.get(url, params=params, headers=headers, timeout=15)
        else:
            res = requests.post(url, json=json_data, timeout=15)
        res.raise_for_status()
        return res
    except Exception as e:
        return None

def run_integrated_pipeline():
    # 1. Base44 농장 정보 업데이트
    print("1️⃣ [Base44] 농장 정보 동기화...")
    res_b44 = safe_request("https://app.base44.com/api/apps/68ccb7f3c0a6ef99bbf4ad23/entities/Farm")
    if res_b44:
        for farm in res_b44.json():
            db['FarmInfo'].update_one({"farm_unique_no": farm['farm_unique_no']}, {"$set": farm}, upsert=True)

    # 2. 개체 수집 및 상태 업데이트
    farms = list(db['FarmInfo'].find({}))
    for farm in farms:
        f_name = farm.get('farm_name', 'Unknown')
        
        # 데이터 정제 강화: 리스트인 경우 첫 번째 요소만 추출
        f_id_raw = farm.get('farm_unique_no', '')
        f_id = str(f_id_raw[0] if isinstance(f_id_raw, list) else f_id_raw).replace('-', '').strip()
        
        f_owner = str(farm.get('owner_name', '')).strip()
        
        f_phone_raw = farm.get('phone', '')
        f_phone = str(f_phone_raw[0] if isinstance(f_phone_raw, list) else f_phone_raw).replace('-', '').strip()
        
        f_manage_no = str(farm.get('external_farm_id', '')).strip()

        if not f_id or not f_owner or not f_phone:
            print(f"⚠️  [{f_name}] 필수 정보 누락으로 스킵 (ID: {f_id}, 소유자: {f_owner}, 전화번호: {f_phone})")
            continue

        is_cnu = "충남대학교" in f_name
        curr_id = CNU_MTRACE_ID if is_cnu else DEFAULT_MTRACE_ID
        curr_key = CNU_MTRACE_KEY if is_cnu else DEFAULT_MTRACE_KEY

        print(f"🚜 [{f_name}] 개체 수집 중... (ID: {f_id}, Owner: {f_owner})")

        # A. 사육 개체
        p_brd = {"userId": curr_id, "apiKey": curr_key, "farmUniqueNo": f_id, "farmerNm": f_owner, "farmerHtelNo": f_phone}
        res_brd = safe_request("https://api.mtrace.go.kr/rest/myFarmData/farmUniqNoCattleBrdIndvd", json_data=p_brd, method='POST')
        
        if res_brd:
            # [디버깅 코드] API 응답이 비어있거나 에러인지 확인
            if "row" not in res_brd.text:
                 print(f"   ❓ 응답 결과 없음: {res_brd.text[:200]}")
            
            try:
                rows = ET.fromstring(res_brd.content).findall(".//row")
                for row in rows:
                    c_no = row.find('animalNo').text
                    db['AnimalMaster'].update_one({"cattleNo": c_no}, {"$set": {"farm_id": f_id, "status": "사육", "last_updated": now}}, upsert=True)
            except: pass

    # 3. 상세 정보 수집 (상세 로그 추가)
    all_animals = list(db['AnimalMaster'].find())
    print(f"2️⃣ 총 {len(all_animals)}두 상세 정보 수집 시작...")

    for animal in all_animals:
        a_no = animal['cattleNo']
        
        # 상세 이력 (1~9)
        history_bundle = {}
        for opt in range(1, 10):
            res_h = safe_request("http://data.ekape.or.kr/openapi-data/service/user/animalTrace/traceNoSearch",
                                 params={"ServiceKey": EKAPE_KEY, "traceNo": a_no, "optionNo": opt})
            if res_h:
                history_bundle[f"opt_{opt}"] = xmltodict.parse(res_h.text)
            time.sleep(0.05)
        
        # 도축 성적
        grade_data = {}
        if animal.get('status') == "도축":
            res_g = safe_request("http://data.ekape.or.kr/openapi-data/service/user/grade/confirm/issueNo", 
                                 params={"serviceKey": EKAPE_KEY, "animalNo": a_no})
            if res_g:
                grade_data = xmltodict.parse(res_g.text)

        db['AnimalHistoryDetail'].update_one(
            {"cattleNo": a_no},
            {"$set": {"history": history_bundle, "grade_result": grade_data, "status": animal.get('status'), "last_updated": now}},
            upsert=True
        )

    print("✅ 모든 공정 완료.")

if __name__ == "__main__":
    run_integrated_pipeline()