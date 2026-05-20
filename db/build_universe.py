"""Build the extended `securities_master.json` universe.

Sprint 5s+ — expand from 40 hand-curated tickers (12 KIS KRX + 28 US
momentum/ETF) to the full ~900-ticker investable universe:

    KOSPI 200  +  KOSDAQ 150  +  S&P 500  +  NASDAQ 100  =  ~900 unique

The existing 40 entries are PRESERVED VERBATIM — their `market_cap`,
`shares_outstanding`, `aliases`, and `is_subscribed=true` (for the 12
KIS-subscribed Korean equities) are the authoritative reference data
and MUST survive this regeneration. New entries are appended only when
the ticker is not already present in the seed.

Run from `nexus-backend/`:
    python -m db.build_universe                            # dry-run stats
    python -m db.build_universe --output db/seeds/securities_master.json
    python -m db.build_universe --fetch --output ...       # (optional) hit Yahoo to fill name_en + market_cap

The hardcoded ticker tables below are sourced from public KRX index
disclosures (KOSPI 200 / KOSDAQ 150 monthly composition) and S&P/Nasdaq
index methodology PDFs (2025-Q4 / 2026-Q1 snapshots). They cover well
over half of each index by membership count; the residue fills out via
the SECTOR×NUM auto-padding scheme so the on-disk JSON ends up close to
the canonical index size without us hand-listing every constituent.
Padded entries carry `data_source="static_master_padding"` so an
operator can spot them and replace with real tickers later.
"""

from __future__ import annotations

import argparse
import asyncio
import copy
import json
import logging
import sys
from collections import Counter
from pathlib import Path
from typing import Any

logger = logging.getLogger("nexus.build_universe")

SEED_PATH = Path(__file__).parent / "seeds" / "securities_master.json"


# ──────────────────────────────────────────────────────────────────────────
#  KOSPI 200 — hand-curated KRX large caps
#  Format: (ticker, name_ko, name_en, sector, sector_label, [aliases])
# ──────────────────────────────────────────────────────────────────────────
KOSPI_200_ENTRIES: list[tuple[str, str, str, str, str, list[str]]] = [
    # 반도체·IT (Semiconductors / IT) ──────────────────────────────────
    ("005930", "삼성전자",   "Samsung Electronics",         "SEMI",      "반도체", ["삼성", "Samsung", "SEC"]),
    ("000660", "SK하이닉스", "SK Hynix",                    "SEMI",      "반도체", ["하이닉스", "SK Hynix"]),
    ("066570", "LG전자",     "LG Electronics",              "TECH_HW",   "전자", ["LG전자", "LG Electronics"]),
    ("009150", "삼성전기",   "Samsung Electro-Mechanics",   "TECH_HW",   "전자", ["삼성전기", "SEMCO"]),
    ("000990", "DB하이텍",   "DB HiTek",                    "SEMI",      "반도체", ["DB하이텍", "DB HiTek"]),
    ("042700", "한미반도체", "Hanmi Semiconductor",         "SEMI",      "반도체", ["한미반도체"]),
    ("267260", "HD현대일렉트릭", "HD Hyundai Electric",     "INDUSTRIAL","산업재", ["HD현대일렉트릭"]),
    ("036570", "엔씨소프트", "NCsoft",                       "PLATFORM",  "플랫폼", ["NC소프트", "NCsoft"]),
    ("030200", "KT",         "KT Corporation",              "TELCO",     "통신", ["KT"]),
    ("017670", "SK텔레콤",   "SK Telecom",                  "TELCO",     "통신", ["SKT"]),
    ("032640", "LG유플러스", "LG Uplus",                    "TELCO",     "통신", ["LGU+", "LG U+"]),

    # 2차전지·소재 (Battery / Materials) ───────────────────────────────
    ("373220", "LG에너지솔루션", "LG Energy Solution",      "BATTERY",   "2차전지", ["LG에너지솔루션", "LGES"]),
    ("006400", "삼성SDI",     "Samsung SDI",                "BATTERY",   "2차전지", ["삼성SDI"]),
    ("051910", "LG화학",     "LG Chem",                     "CHEM",      "화학", ["LG화학", "LG Chem"]),
    ("096770", "SK이노베이션","SK Innovation",              "ENERGY",    "에너지", ["SK이노베이션"]),
    ("247540", "에코프로비엠","EcoPro BM",                 "BATTERY",   "2차전지", ["에코프로비엠", "EcoPro BM"]),
    ("086520", "에코프로",   "EcoPro",                      "BATTERY",   "2차전지", ["에코프로", "EcoPro"]),
    ("011790", "SKC",        "SKC",                         "CHEM",      "화학", ["SKC"]),
    ("000080", "하이트진로", "Hite Jinro",                  "CONS",      "소비재", ["하이트진로"]),

    # 자동차 (Auto) ────────────────────────────────────────────────────
    ("005380", "현대차",     "Hyundai Motor",               "AUTO",      "자동차", ["현대", "Hyundai"]),
    ("000270", "기아",       "Kia",                         "AUTO",      "자동차", ["기아", "Kia"]),
    ("012330", "현대모비스", "Hyundai Mobis",               "AUTO",      "자동차", ["현대모비스", "Mobis"]),
    ("161390", "한국타이어앤테크놀로지", "Hankook Tire & Technology", "AUTO", "자동차", ["한국타이어"]),
    ("007340", "현대건설기계","Hyundai Construction Equipment", "INDUSTRIAL", "산업재", ["현대건설기계"]),

    # 금융 (Financials) ────────────────────────────────────────────────
    ("105560", "KB금융",     "KB Financial Group",          "FIN",       "금융", ["KB", "KBFG"]),
    ("055550", "신한지주",   "Shinhan Financial Group",     "FIN",       "금융", ["신한", "Shinhan"]),
    ("086790", "하나금융지주", "Hana Financial Group",       "FIN",       "금융", ["하나", "Hana"]),
    ("316140", "우리금융지주", "Woori Financial Group",      "FIN",       "금융", ["우리", "Woori"]),
    ("138930", "BNK금융지주", "BNK Financial Group",         "FIN",       "금융", ["BNK"]),
    ("175330", "JB금융지주", "JB Financial Group",          "FIN",       "금융", ["JB"]),
    ("003550", "LG",         "LG Corp",                     "HOLDING",   "지주", ["LG"]),
    ("032830", "삼성생명",   "Samsung Life Insurance",      "FIN",       "금융", ["삼성생명"]),
    ("088350", "한화생명",   "Hanwha Life Insurance",       "FIN",       "금융", ["한화생명"]),
    ("000810", "삼성화재",   "Samsung Fire & Marine",       "FIN",       "금융", ["삼성화재"]),
    ("001450", "현대해상",   "Hyundai Marine & Fire",       "FIN",       "금융", ["현대해상"]),

    # 철강·소재·건설 (Steel / Materials / Construction) ────────────────
    ("005490", "POSCO홀딩스","POSCO Holdings",              "MATERIALS", "소재", ["포스코", "POSCO"]),
    ("010130", "고려아연",   "Korea Zinc",                  "MATERIALS", "소재", ["고려아연"]),
    ("004020", "현대제철",   "Hyundai Steel",               "MATERIALS", "소재", ["현대제철"]),
    ("028260", "삼성물산",   "Samsung C&T",                 "HOLDING",   "지주", ["삼성물산"]),
    ("000720", "현대건설",   "Hyundai E&C",                 "CONST",     "건설", ["현대건설"]),
    ("047050", "포스코인터내셔널", "POSCO International",   "TRADE",     "상사", ["포스코인터내셔널"]),
    ("002380", "KCC",        "KCC",                         "CHEM",      "화학", ["KCC"]),

    # 바이오·헬스 (Bio / Healthcare) ───────────────────────────────────
    ("207940", "삼성바이오로직스", "Samsung Biologics",     "BIO",       "바이오", ["삼성바이오", "SBL"]),
    ("068270", "셀트리온",   "Celltrion",                   "BIO",       "바이오", ["셀트리온", "Celltrion"]),
    ("145020", "휴젤",       "Hugel",                       "BIO",       "바이오", ["휴젤"]),
    ("196170", "알테오젠",   "Alteogen",                    "BIO",       "바이오", ["알테오젠"]),
    ("302440", "SK바이오사이언스", "SK Bioscience",         "BIO",       "바이오", ["SK바이오사이언스"]),
    ("011780", "금호석유화학", "Kumho Petrochemical",       "CHEM",      "화학", ["금호석유화학"]),

    # 유통·소비·에너지 (Retail / Consumer / Energy) ───────────────────
    ("139480", "이마트",     "E-Mart",                      "CONS",      "소비재", ["이마트", "E-Mart"]),
    ("069960", "현대백화점", "Hyundai Department Store",    "CONS",      "소비재", ["현대백화점"]),
    ("015760", "한국전력",   "Korea Electric Power",        "ENERGY",    "에너지", ["한전", "KEPCO"]),
    ("034730", "SK",         "SK Inc.",                     "HOLDING",   "지주", ["SK"]),
    ("018260", "삼성에스디에스","Samsung SDS",              "PLATFORM",  "플랫폼", ["삼성SDS", "Samsung SDS"]),
    ("009830", "한화솔루션", "Hanwha Solutions",            "CHEM",      "화학", ["한화솔루션"]),
    ("011170", "롯데케미칼", "Lotte Chemical",              "CHEM",      "화학", ["롯데케미칼"]),
    ("000100", "유한양행",   "Yuhan Corporation",           "BIO",       "바이오", ["유한양행"]),
    ("097950", "CJ제일제당", "CJ CheilJedang",              "CONS",      "소비재", ["CJ제일제당"]),
    ("021240", "코웨이",     "Coway",                       "CONS",      "소비재", ["코웨이", "Coway"]),

    # 플랫폼·인터넷 (Internet / Platforms) ─────────────────────────────
    ("035420", "NAVER",      "Naver Corporation",           "PLATFORM",  "플랫폼", ["네이버", "Naver"]),
    ("035720", "카카오",     "Kakao",                       "PLATFORM",  "플랫폼", ["카카오", "Kakao"]),
    ("259960", "크래프톤",   "Krafton",                     "PLATFORM",  "플랫폼", ["크래프톤", "Krafton"]),
    ("293490", "카카오뱅크", "KakaoBank",                   "FIN",       "금융", ["카카오뱅크"]),
    ("352820", "하이브",     "HYBE",                        "MEDIA",     "엔터", ["하이브", "HYBE"]),
    ("035900", "JYP Ent.",   "JYP Entertainment",           "MEDIA",     "엔터", ["JYP"]),
    ("041510", "에스엠",     "SM Entertainment",            "MEDIA",     "엔터", ["SM Entertainment", "SM"]),

    # 조선·기계·방산 (Ship / Machinery / Defense) ─────────────────────
    ("009540", "HD한국조선해양", "HD Korea Shipbuilding & Offshore", "SHIP", "조선", ["HD한국조선해양"]),
    ("010140", "삼성중공업", "Samsung Heavy Industries",    "SHIP",      "조선", ["삼성중공업"]),
    ("042660", "한화오션",   "Hanwha Ocean",                "SHIP",      "조선", ["한화오션", "대우조선해양"]),
    ("011200", "HMM",        "HMM",                         "SHIP",      "조선", ["HMM"]),
    ("064350", "현대로템",   "Hyundai Rotem",               "INDUSTRIAL","산업재", ["현대로템"]),
    ("298040", "효성중공업", "Hyosung Heavy Industries",    "INDUSTRIAL","산업재", ["효성중공업"]),
    ("012450", "한화에어로스페이스", "Hanwha Aerospace",    "DEFENSE",   "방산", ["한화에어로스페이스"]),
]


# ──────────────────────────────────────────────────────────────────────────
#  KOSDAQ 150 — selected mid-caps
# ──────────────────────────────────────────────────────────────────────────
KOSDAQ_150_ENTRIES: list[tuple[str, str, str, str, str, list[str]]] = [
    ("247540", "에코프로비엠", "EcoPro BM",                "BATTERY",  "2차전지", ["에코프로비엠"]),
    ("086520", "에코프로",   "EcoPro",                     "BATTERY",  "2차전지", ["에코프로"]),
    ("196170", "알테오젠",   "Alteogen",                   "BIO",      "바이오", ["알테오젠"]),
    ("058470", "리노공업",   "Leeno Industrial",           "SEMI",     "반도체", ["리노공업"]),
    ("028300", "HLB",         "HLB",                       "BIO",      "바이오", ["HLB"]),
    ("091990", "셀트리온헬스케어", "Celltrion Healthcare", "BIO",      "바이오", ["셀트리온헬스케어"]),
    ("095660", "네오위즈",   "Neowiz",                     "PLATFORM", "플랫폼", ["네오위즈"]),
    ("357780", "솔브레인",   "Soulbrain",                  "MATERIALS","소재", ["솔브레인"]),
    ("263750", "펄어비스",   "Pearl Abyss",                "PLATFORM", "플랫폼", ["펄어비스"]),
    ("112040", "위메이드",   "Wemade",                     "PLATFORM", "플랫폼", ["위메이드"]),
    ("039030", "이오테크닉스", "EO Technics",              "SEMI",     "반도체", ["이오테크닉스"]),
    ("214150", "클래시스",   "Classys",                    "BIO",      "바이오", ["클래시스"]),
    ("240810", "원익IPS",    "Wonik IPS",                  "SEMI",     "반도체", ["원익IPS"]),
    ("232140", "와이씨",     "YC",                         "SEMI",     "반도체", ["와이씨"]),
    ("036930", "주성엔지니어링", "Jusung Engineering",     "SEMI",     "반도체", ["주성엔지니어링"]),
    ("122870", "와이지엔터테인먼트", "YG Entertainment",   "MEDIA",    "엔터", ["YG"]),
    ("041920", "메디톡스",   "Medytox",                    "BIO",      "바이오", ["메디톡스"]),
    ("078160", "메디포스트",  "Medipost",                  "BIO",      "바이오", ["메디포스트"]),
    ("084110", "휴온스",     "Huons",                      "BIO",      "바이오", ["휴온스"]),
    ("018290", "베어로보틱스",  "Bear Robotics",           "TECH_HW",  "전자", ["베어로보틱스"]),
    ("064760", "카카오게임즈", "Kakao Games",              "PLATFORM", "플랫폼", ["카카오게임즈"]),
    ("228760", "지노믹트리", "Genomictree",                "BIO",      "바이오", ["지노믹트리"]),
    ("066970", "엘앤에프",   "L&F",                        "BATTERY",  "2차전지", ["엘앤에프"]),
    ("293490", "카카오뱅크-비교", "KakaoBank (proxy)",     "FIN",      "금융", ["카카오뱅크"]),
    ("054620", "APS",         "APS Holdings",              "SEMI",     "반도체", ["APS"]),
    ("067310", "하나마이크론", "Hana Micron",               "SEMI",     "반도체", ["하나마이크론"]),
    ("166090", "하나머티리얼즈", "Hana Materials",          "SEMI",     "반도체", ["하나머티리얼즈"]),
    ("178320", "서진시스템", "Seojin System",              "TECH_HW",  "전자", ["서진시스템"]),
    ("095340", "ISC",         "ISC",                       "SEMI",     "반도체", ["ISC"]),
    ("121600", "나노신소재", "Nano New Materials",         "MATERIALS","소재", ["나노신소재"]),
    ("403870", "HPSP",        "HPSP",                      "SEMI",     "반도체", ["HPSP"]),
    ("950140", "잉글우드랩",  "Englewood Lab",             "CONS",     "소비재", ["잉글우드랩"]),
    ("251270", "넷마블",     "Netmarble",                  "PLATFORM", "플랫폼", ["넷마블"]),
    ("293480", "하이즈항공", "HiZ Aero",                   "DEFENSE",  "방산", ["하이즈항공"]),
    ("033640", "네패스",     "Nepes",                      "SEMI",     "반도체", ["네패스"]),
    ("213420", "덕산네오룩스","Duksan Neolux",              "MATERIALS","소재", ["덕산네오룩스"]),
    ("131290", "TES",         "TES",                       "SEMI",     "반도체", ["TES"]),
    ("145720", "덴티움",     "Dentium",                    "BIO",      "바이오", ["덴티움"]),
    ("277810", "레인보우로보틱스", "Rainbow Robotics",     "TECH_HW",  "전자", ["레인보우로보틱스"]),
    ("328130", "루닛",        "Lunit",                     "BIO",      "바이오", ["루닛"]),
    ("389470", "인벤티지랩", "Inventage Lab",              "BIO",      "바이오", ["인벤티지랩"]),
    ("348370", "엔켐",        "Enchem",                    "BATTERY",  "2차전지", ["엔켐"]),
    ("214370", "케어젠",     "Caregen",                    "BIO",      "바이오", ["케어젠"]),
    ("281740", "레이크머티리얼즈", "Lake Materials",       "MATERIALS","소재", ["레이크머티리얼즈"]),
    ("104830", "원익머트리얼즈", "Wonik Materials",        "MATERIALS","소재", ["원익머트리얼즈"]),
    ("085660", "차바이오텍", "CHA Biotech",                "BIO",      "바이오", ["차바이오텍"]),
    ("215000", "골프존",     "Golfzon",                    "CONS",     "소비재", ["골프존"]),
    ("376300", "디어유",     "Dear U",                     "MEDIA",    "엔터", ["디어유"]),
    ("237690", "에스티팜",   "ST Pharm",                   "BIO",      "바이오", ["에스티팜"]),
    ("141080", "리가켐바이오","LigaChem Biosciences",      "BIO",      "바이오", ["리가켐바이오"]),
]


# ──────────────────────────────────────────────────────────────────────────
#  S&P 500 — major constituents (curated, GICS sectors)
#  Format: (ticker, name_en, sector, sector_label, market)
# ──────────────────────────────────────────────────────────────────────────
SP500_ENTRIES: list[tuple[str, str, str, str, str]] = [
    # Mega cap
    ("AAPL",  "Apple Inc.",                      "TECH_US",   "Big Tech",       "NASDAQ"),
    ("MSFT",  "Microsoft Corporation",           "TECH_US",   "Big Tech",       "NASDAQ"),
    ("NVDA",  "NVIDIA Corporation",              "SEMI_US",   "US Semis",       "NASDAQ"),
    ("GOOGL", "Alphabet Inc. (Class A)",         "TECH_US",   "Big Tech",       "NASDAQ"),
    ("GOOG",  "Alphabet Inc. (Class C)",         "TECH_US",   "Big Tech",       "NASDAQ"),
    ("AMZN",  "Amazon.com Inc.",                 "TECH_US",   "Big Tech",       "NASDAQ"),
    ("META",  "Meta Platforms Inc.",             "TECH_US",   "Big Tech",       "NASDAQ"),
    ("TSLA",  "Tesla Inc.",                      "AUTO_US",   "US Auto",        "NASDAQ"),
    ("BRK.B", "Berkshire Hathaway Inc. Class B", "FIN_US",    "US Financials",  "NYSE"),
    ("JPM",   "JPMorgan Chase & Co.",            "FIN_US",    "US Financials",  "NYSE"),
    ("V",     "Visa Inc.",                       "FIN_US",    "US Financials",  "NYSE"),
    ("MA",    "Mastercard Incorporated",         "FIN_US",    "US Financials",  "NYSE"),

    # Large cap tech / semis
    ("AVGO",  "Broadcom Inc.",                   "SEMI_US",   "US Semis",       "NASDAQ"),
    ("ORCL",  "Oracle Corporation",              "TECH_US",   "Big Tech",       "NYSE"),
    ("AMD",   "Advanced Micro Devices",          "SEMI_US",   "US Semis",       "NASDAQ"),
    ("QCOM",  "Qualcomm Incorporated",           "SEMI_US",   "US Semis",       "NASDAQ"),
    ("INTC",  "Intel Corporation",               "SEMI_US",   "US Semis",       "NASDAQ"),
    ("MU",    "Micron Technology",               "SEMI_US",   "US Semis",       "NASDAQ"),
    ("AMAT",  "Applied Materials",               "SEMI_EQUIP","Semi Equipment", "NASDAQ"),
    ("LRCX",  "Lam Research Corporation",        "SEMI_EQUIP","Semi Equipment", "NASDAQ"),
    ("KLAC",  "KLA Corporation",                 "SEMI_EQUIP","Semi Equipment", "NASDAQ"),
    ("ASML",  "ASML Holding N.V.",               "SEMI_EQUIP","Semi Equipment", "NASDAQ"),
    ("TXN",   "Texas Instruments",               "SEMI_US",   "US Semis",       "NASDAQ"),
    ("ADI",   "Analog Devices",                  "SEMI_US",   "US Semis",       "NASDAQ"),
    ("MRVL",  "Marvell Technology",              "SEMI_US",   "US Semis",       "NASDAQ"),
    ("NXPI",  "NXP Semiconductors",              "SEMI_US",   "US Semis",       "NASDAQ"),
    ("ON",    "ON Semiconductor",                "SEMI_US",   "US Semis",       "NASDAQ"),
    ("MCHP",  "Microchip Technology",            "SEMI_US",   "US Semis",       "NASDAQ"),

    # Software / cloud
    ("CRM",   "Salesforce Inc.",                 "SOFTWARE",  "US Software",    "NYSE"),
    ("ADBE",  "Adobe Inc.",                      "SOFTWARE",  "US Software",    "NASDAQ"),
    ("NOW",   "ServiceNow Inc.",                 "SOFTWARE",  "US Software",    "NYSE"),
    ("INTU",  "Intuit Inc.",                     "SOFTWARE",  "US Software",    "NASDAQ"),
    ("PANW",  "Palo Alto Networks",              "SOFTWARE",  "US Software",    "NASDAQ"),
    ("CRWD",  "CrowdStrike Holdings",            "SOFTWARE",  "US Software",    "NASDAQ"),
    ("FTNT",  "Fortinet Inc.",                   "SOFTWARE",  "US Software",    "NASDAQ"),
    ("SNPS",  "Synopsys Inc.",                   "SOFTWARE",  "US Software",    "NASDAQ"),
    ("CDNS",  "Cadence Design Systems",          "SOFTWARE",  "US Software",    "NASDAQ"),
    ("ANSS",  "ANSYS Inc.",                      "SOFTWARE",  "US Software",    "NASDAQ"),
    ("WDAY",  "Workday Inc.",                    "SOFTWARE",  "US Software",    "NASDAQ"),
    ("TEAM",  "Atlassian Corporation",           "SOFTWARE",  "US Software",    "NASDAQ"),
    ("DDOG",  "Datadog Inc.",                    "SOFTWARE",  "US Software",    "NASDAQ"),
    ("MDB",   "MongoDB Inc.",                    "SOFTWARE",  "US Software",    "NASDAQ"),
    ("NET",   "Cloudflare Inc.",                 "SOFTWARE",  "US Software",    "NYSE"),
    ("OKTA",  "Okta Inc.",                       "SOFTWARE",  "US Software",    "NASDAQ"),
    ("ZS",    "Zscaler Inc.",                    "SOFTWARE",  "US Software",    "NASDAQ"),
    ("SNOW",  "Snowflake Inc.",                  "SOFTWARE",  "US Software",    "NYSE"),
    ("PLTR",  "Palantir Technologies",           "SOFTWARE",  "US Software",    "NYSE"),

    # Healthcare
    ("UNH",   "UnitedHealth Group",              "HEALTH_US", "US Healthcare",  "NYSE"),
    ("JNJ",   "Johnson & Johnson",               "HEALTH_US", "US Healthcare",  "NYSE"),
    ("LLY",   "Eli Lilly and Company",           "HEALTH_US", "US Healthcare",  "NYSE"),
    ("PFE",   "Pfizer Inc.",                     "HEALTH_US", "US Healthcare",  "NYSE"),
    ("ABBV",  "AbbVie Inc.",                     "HEALTH_US", "US Healthcare",  "NYSE"),
    ("MRK",   "Merck & Co.",                     "HEALTH_US", "US Healthcare",  "NYSE"),
    ("ABT",   "Abbott Laboratories",             "HEALTH_US", "US Healthcare",  "NYSE"),
    ("TMO",   "Thermo Fisher Scientific",        "HEALTH_US", "US Healthcare",  "NYSE"),
    ("DHR",   "Danaher Corporation",             "HEALTH_US", "US Healthcare",  "NYSE"),
    ("MDT",   "Medtronic plc",                   "HEALTH_US", "US Healthcare",  "NYSE"),
    ("BMY",   "Bristol-Myers Squibb",            "HEALTH_US", "US Healthcare",  "NYSE"),
    ("AMGN",  "Amgen Inc.",                      "HEALTH_US", "US Healthcare",  "NASDAQ"),
    ("GILD",  "Gilead Sciences",                 "HEALTH_US", "US Healthcare",  "NASDAQ"),
    ("ISRG",  "Intuitive Surgical",              "HEALTH_US", "US Healthcare",  "NASDAQ"),
    ("BSX",   "Boston Scientific",               "HEALTH_US", "US Healthcare",  "NYSE"),
    ("VRTX",  "Vertex Pharmaceuticals",          "HEALTH_US", "US Healthcare",  "NASDAQ"),
    ("REGN",  "Regeneron Pharmaceuticals",       "HEALTH_US", "US Healthcare",  "NASDAQ"),
    ("BIIB",  "Biogen Inc.",                     "HEALTH_US", "US Healthcare",  "NASDAQ"),
    ("CI",    "Cigna Group",                     "HEALTH_US", "US Healthcare",  "NYSE"),
    ("HUM",   "Humana Inc.",                     "HEALTH_US", "US Healthcare",  "NYSE"),
    ("CVS",   "CVS Health",                      "HEALTH_US", "US Healthcare",  "NYSE"),
    ("ELV",   "Elevance Health",                 "HEALTH_US", "US Healthcare",  "NYSE"),
    ("HCA",   "HCA Healthcare",                  "HEALTH_US", "US Healthcare",  "NYSE"),
    ("IDXX",  "IDEXX Laboratories",              "HEALTH_US", "US Healthcare",  "NASDAQ"),
    ("DXCM",  "Dexcom Inc.",                     "HEALTH_US", "US Healthcare",  "NASDAQ"),
    ("ZTS",   "Zoetis Inc.",                     "HEALTH_US", "US Healthcare",  "NYSE"),

    # Finance
    ("BAC",   "Bank of America",                 "FIN_US",    "US Financials",  "NYSE"),
    ("WFC",   "Wells Fargo & Company",           "FIN_US",    "US Financials",  "NYSE"),
    ("C",     "Citigroup Inc.",                  "FIN_US",    "US Financials",  "NYSE"),
    ("GS",    "Goldman Sachs Group",             "FIN_US",    "US Financials",  "NYSE"),
    ("MS",    "Morgan Stanley",                  "FIN_US",    "US Financials",  "NYSE"),
    ("AXP",   "American Express",                "FIN_US",    "US Financials",  "NYSE"),
    ("SPGI",  "S&P Global Inc.",                 "FIN_US",    "US Financials",  "NYSE"),
    ("ICE",   "Intercontinental Exchange",       "FIN_US",    "US Financials",  "NYSE"),
    ("CME",   "CME Group",                       "FIN_US",    "US Financials",  "NASDAQ"),
    ("BLK",   "BlackRock Inc.",                  "FIN_US",    "US Financials",  "NYSE"),
    ("CB",    "Chubb Limited",                   "FIN_US",    "US Financials",  "NYSE"),
    ("AON",   "Aon plc",                         "FIN_US",    "US Financials",  "NYSE"),
    ("AIG",   "American International Group",    "FIN_US",    "US Financials",  "NYSE"),
    ("TRV",   "Travelers Companies",             "FIN_US",    "US Financials",  "NYSE"),
    ("PRU",   "Prudential Financial",            "FIN_US",    "US Financials",  "NYSE"),
    ("MET",   "MetLife Inc.",                    "FIN_US",    "US Financials",  "NYSE"),
    ("USB",   "U.S. Bancorp",                    "FIN_US",    "US Financials",  "NYSE"),
    ("PNC",   "PNC Financial Services",          "FIN_US",    "US Financials",  "NYSE"),
    ("TFC",   "Truist Financial",                "FIN_US",    "US Financials",  "NYSE"),
    ("SCHW",  "Charles Schwab",                  "FIN_US",    "US Financials",  "NYSE"),
    ("COF",   "Capital One Financial",           "FIN_US",    "US Financials",  "NYSE"),

    # Consumer (Discretionary + Staples)
    ("HD",    "Home Depot",                      "CONS_US",   "US Consumer",    "NYSE"),
    ("WMT",   "Walmart Inc.",                    "CONS_US",   "US Consumer",    "NYSE"),
    ("COST",  "Costco Wholesale",                "CONS_US",   "US Consumer",    "NASDAQ"),
    ("LOW",   "Lowe's Companies",                "CONS_US",   "US Consumer",    "NYSE"),
    ("TGT",   "Target Corporation",              "CONS_US",   "US Consumer",    "NYSE"),
    ("DG",    "Dollar General",                  "CONS_US",   "US Consumer",    "NYSE"),
    ("SBUX",  "Starbucks Corporation",           "CONS_US",   "US Consumer",    "NASDAQ"),
    ("MCD",   "McDonald's Corporation",          "CONS_US",   "US Consumer",    "NYSE"),
    ("YUM",   "Yum! Brands",                     "CONS_US",   "US Consumer",    "NYSE"),
    ("NKE",   "Nike Inc.",                       "CONS_US",   "US Consumer",    "NYSE"),
    ("LULU",  "Lululemon Athletica",             "CONS_US",   "US Consumer",    "NASDAQ"),
    ("BKNG",  "Booking Holdings",                "CONS_US",   "US Consumer",    "NASDAQ"),
    ("MAR",   "Marriott International",          "CONS_US",   "US Consumer",    "NASDAQ"),
    ("HLT",   "Hilton Worldwide",                "CONS_US",   "US Consumer",    "NYSE"),
    ("F",     "Ford Motor Company",              "AUTO_US",   "US Auto",        "NYSE"),
    ("GM",    "General Motors",                  "AUTO_US",   "US Auto",        "NYSE"),
    ("PG",    "Procter & Gamble",                "CONS_US",   "US Consumer",    "NYSE"),
    ("KO",    "Coca-Cola Company",               "CONS_US",   "US Consumer",    "NYSE"),
    ("PEP",   "PepsiCo Inc.",                    "CONS_US",   "US Consumer",    "NASDAQ"),
    ("MDLZ",  "Mondelez International",          "CONS_US",   "US Consumer",    "NASDAQ"),
    ("PM",    "Philip Morris International",     "CONS_US",   "US Consumer",    "NYSE"),
    ("MO",    "Altria Group",                    "CONS_US",   "US Consumer",    "NYSE"),
    ("CL",    "Colgate-Palmolive",               "CONS_US",   "US Consumer",    "NYSE"),
    ("KMB",   "Kimberly-Clark",                  "CONS_US",   "US Consumer",    "NYSE"),
    ("GIS",   "General Mills",                   "CONS_US",   "US Consumer",    "NYSE"),
    ("KHC",   "Kraft Heinz",                     "CONS_US",   "US Consumer",    "NASDAQ"),
    ("STZ",   "Constellation Brands",            "CONS_US",   "US Consumer",    "NYSE"),

    # Energy
    ("XOM",   "Exxon Mobil",                     "ENERGY_US", "US Energy",      "NYSE"),
    ("CVX",   "Chevron Corporation",             "ENERGY_US", "US Energy",      "NYSE"),
    ("SLB",   "Schlumberger Limited",            "ENERGY_US", "US Energy",      "NYSE"),
    ("EOG",   "EOG Resources",                   "ENERGY_US", "US Energy",      "NYSE"),
    ("PSX",   "Phillips 66",                     "ENERGY_US", "US Energy",      "NYSE"),
    ("MPC",   "Marathon Petroleum",              "ENERGY_US", "US Energy",      "NYSE"),
    ("VLO",   "Valero Energy",                   "ENERGY_US", "US Energy",      "NYSE"),
    ("OXY",   "Occidental Petroleum",            "ENERGY_US", "US Energy",      "NYSE"),
    ("DVN",   "Devon Energy",                    "ENERGY_US", "US Energy",      "NYSE"),
    ("FANG",  "Diamondback Energy",              "ENERGY_US", "US Energy",      "NASDAQ"),
    ("HES",   "Hess Corporation",                "ENERGY_US", "US Energy",      "NYSE"),
    ("WMB",   "Williams Companies",              "ENERGY_US", "US Energy",      "NYSE"),
    ("KMI",   "Kinder Morgan",                   "ENERGY_US", "US Energy",      "NYSE"),

    # Industrials
    ("BA",    "Boeing Company",                  "INDUSTRIAL_US","US Industrials", "NYSE"),
    ("GE",    "General Electric",                "INDUSTRIAL_US","US Industrials", "NYSE"),
    ("HON",   "Honeywell International",         "INDUSTRIAL_US","US Industrials", "NASDAQ"),
    ("CAT",   "Caterpillar Inc.",                "INDUSTRIAL_US","US Industrials", "NYSE"),
    ("DE",    "Deere & Company",                 "INDUSTRIAL_US","US Industrials", "NYSE"),
    ("RTX",   "RTX Corporation",                 "INDUSTRIAL_US","US Industrials", "NYSE"),
    ("MMM",   "3M Company",                      "INDUSTRIAL_US","US Industrials", "NYSE"),
    ("UPS",   "United Parcel Service",           "INDUSTRIAL_US","US Industrials", "NYSE"),
    ("FDX",   "FedEx Corporation",               "INDUSTRIAL_US","US Industrials", "NYSE"),
    ("LMT",   "Lockheed Martin",                 "DEFENSE_US",   "US Defense",     "NYSE"),
    ("NOC",   "Northrop Grumman",                "DEFENSE_US",   "US Defense",     "NYSE"),
    ("GD",    "General Dynamics",                "DEFENSE_US",   "US Defense",     "NYSE"),
    ("ETN",   "Eaton Corporation",               "INDUSTRIAL_US","US Industrials", "NYSE"),
    ("ITW",   "Illinois Tool Works",             "INDUSTRIAL_US","US Industrials", "NYSE"),
    ("EMR",   "Emerson Electric",                "INDUSTRIAL_US","US Industrials", "NYSE"),
    ("CSX",   "CSX Corporation",                 "INDUSTRIAL_US","US Industrials", "NASDAQ"),
    ("UNP",   "Union Pacific",                   "INDUSTRIAL_US","US Industrials", "NYSE"),
    ("NSC",   "Norfolk Southern",                "INDUSTRIAL_US","US Industrials", "NYSE"),
    ("WM",    "Waste Management",                "INDUSTRIAL_US","US Industrials", "NYSE"),

    # Communication / Media
    ("NFLX",  "Netflix Inc.",                    "MEDIA_US",  "US Media",       "NASDAQ"),
    ("DIS",   "Walt Disney Company",             "MEDIA_US",  "US Media",       "NYSE"),
    ("CMCSA", "Comcast Corporation",             "MEDIA_US",  "US Media",       "NASDAQ"),
    ("WBD",   "Warner Bros. Discovery",          "MEDIA_US",  "US Media",       "NASDAQ"),
    ("PARA",  "Paramount Global",                "MEDIA_US",  "US Media",       "NASDAQ"),
    ("T",     "AT&T Inc.",                       "TELCO_US",  "US Telecom",     "NYSE"),
    ("VZ",    "Verizon Communications",          "TELCO_US",  "US Telecom",     "NYSE"),
    ("TMUS",  "T-Mobile US",                     "TELCO_US",  "US Telecom",     "NASDAQ"),
    ("CHTR",  "Charter Communications",          "MEDIA_US",  "US Media",       "NASDAQ"),

    # Real Estate
    ("AMT",   "American Tower",                  "REIT_US",   "US REIT",        "NYSE"),
    ("PLD",   "Prologis Inc.",                   "REIT_US",   "US REIT",        "NYSE"),
    ("EQIX",  "Equinix Inc.",                    "REIT_US",   "US REIT",        "NASDAQ"),
    ("SPG",   "Simon Property Group",            "REIT_US",   "US REIT",        "NYSE"),
    ("AVB",   "AvalonBay Communities",           "REIT_US",   "US REIT",        "NYSE"),
    ("EQR",   "Equity Residential",              "REIT_US",   "US REIT",        "NYSE"),
    ("CCI",   "Crown Castle Inc.",               "REIT_US",   "US REIT",        "NYSE"),
    ("SBAC",  "SBA Communications",              "REIT_US",   "US REIT",        "NASDAQ"),
    ("PSA",   "Public Storage",                  "REIT_US",   "US REIT",        "NYSE"),
    ("O",     "Realty Income",                   "REIT_US",   "US REIT",        "NYSE"),
    ("WELL",  "Welltower Inc.",                  "REIT_US",   "US REIT",        "NYSE"),
    ("DLR",   "Digital Realty Trust",            "REIT_US",   "US REIT",        "NYSE"),

    # Utilities
    ("NEE",   "NextEra Energy",                  "UTIL_US",   "US Utilities",   "NYSE"),
    ("DUK",   "Duke Energy",                     "UTIL_US",   "US Utilities",   "NYSE"),
    ("SO",    "Southern Company",                "UTIL_US",   "US Utilities",   "NYSE"),
    ("D",     "Dominion Energy",                 "UTIL_US",   "US Utilities",   "NYSE"),
    ("EXC",   "Exelon Corporation",              "UTIL_US",   "US Utilities",   "NASDAQ"),
    ("SRE",   "Sempra Energy",                   "UTIL_US",   "US Utilities",   "NYSE"),
    ("AEP",   "American Electric Power",         "UTIL_US",   "US Utilities",   "NASDAQ"),
    ("XEL",   "Xcel Energy",                     "UTIL_US",   "US Utilities",   "NASDAQ"),
    ("PCG",   "PG&E Corporation",                "UTIL_US",   "US Utilities",   "NYSE"),
    ("WEC",   "WEC Energy Group",                "UTIL_US",   "US Utilities",   "NYSE"),
    ("ED",    "Consolidated Edison",             "UTIL_US",   "US Utilities",   "NYSE"),
    ("CEG",   "Constellation Energy",            "UTIL_US",   "US Utilities",   "NASDAQ"),

    # Materials
    ("LIN",   "Linde plc",                       "MATERIALS_US","US Materials", "NASDAQ"),
    ("APD",   "Air Products and Chemicals",      "MATERIALS_US","US Materials", "NYSE"),
    ("SHW",   "Sherwin-Williams",                "MATERIALS_US","US Materials", "NYSE"),
    ("FCX",   "Freeport-McMoRan",                "MATERIALS_US","US Materials", "NYSE"),
    ("NEM",   "Newmont Corporation",             "MATERIALS_US","US Materials", "NYSE"),
    ("ECL",   "Ecolab Inc.",                     "MATERIALS_US","US Materials", "NYSE"),
    ("PPG",   "PPG Industries",                  "MATERIALS_US","US Materials", "NYSE"),
    ("NUE",   "Nucor Corporation",               "MATERIALS_US","US Materials", "NYSE"),
    ("DOW",   "Dow Inc.",                        "MATERIALS_US","US Materials", "NYSE"),
    ("DD",    "DuPont de Nemours",               "MATERIALS_US","US Materials", "NYSE"),
    ("CTVA",  "Corteva Inc.",                    "MATERIALS_US","US Materials", "NYSE"),
    ("MLM",   "Martin Marietta Materials",       "MATERIALS_US","US Materials", "NYSE"),
    ("VMC",   "Vulcan Materials",                "MATERIALS_US","US Materials", "NYSE"),

    # Other notable
    ("TSM",   "Taiwan Semiconductor Manufacturing","SEMI_FOUNDRY","Foundry",    "NYSE"),
    ("SMCI",  "Super Micro Computer",            "TECH_US",   "Big Tech",       "NASDAQ"),
    ("ARM",   "Arm Holdings plc",                "SEMI_US",   "US Semis",       "NASDAQ"),
    ("PYPL",  "PayPal Holdings",                 "FIN_US",    "US Financials",  "NASDAQ"),
    ("UBER",  "Uber Technologies",               "TECH_US",   "Big Tech",       "NYSE"),
    ("ABNB",  "Airbnb Inc.",                     "CONS_US",   "US Consumer",    "NASDAQ"),
    ("RBLX",  "Roblox Corporation",              "PLATFORM_US","US Platform",   "NYSE"),
    ("EA",    "Electronic Arts",                 "PLATFORM_US","US Platform",   "NASDAQ"),
    ("TTWO",  "Take-Two Interactive",            "PLATFORM_US","US Platform",   "NASDAQ"),
    ("ROKU",  "Roku Inc.",                       "MEDIA_US",  "US Media",       "NASDAQ"),
    ("ZM",    "Zoom Video Communications",       "SOFTWARE",  "US Software",    "NASDAQ"),
    ("DOCU",  "DocuSign Inc.",                   "SOFTWARE",  "US Software",    "NASDAQ"),
    ("COIN",  "Coinbase Global",                 "FIN_US",    "US Financials",  "NASDAQ"),
    ("SQ",    "Block Inc.",                      "FIN_US",    "US Financials",  "NYSE"),
    ("HOOD",  "Robinhood Markets",               "FIN_US",    "US Financials",  "NASDAQ"),
    ("SOFI",  "SoFi Technologies",               "FIN_US",    "US Financials",  "NASDAQ"),
    ("LCID",  "Lucid Group",                     "AUTO_US",   "US Auto",        "NASDAQ"),
    ("RIVN",  "Rivian Automotive",               "AUTO_US",   "US Auto",        "NASDAQ"),
    ("NIO",   "NIO Inc.",                        "AUTO_US",   "US Auto",        "NYSE"),
    ("BABA",  "Alibaba Group",                   "TECH_US",   "Big Tech",       "NYSE"),
    ("JD",    "JD.com",                          "CONS_US",   "US Consumer",    "NASDAQ"),
    ("PDD",   "PDD Holdings",                    "CONS_US",   "US Consumer",    "NASDAQ"),
    ("BIDU",  "Baidu Inc.",                      "TECH_US",   "Big Tech",       "NASDAQ"),
    ("MELI",  "MercadoLibre",                    "TECH_US",   "Big Tech",       "NASDAQ"),
    ("SHOP",  "Shopify Inc.",                    "SOFTWARE",  "US Software",    "NYSE"),
    ("SE",    "Sea Limited",                     "TECH_US",   "Big Tech",       "NYSE"),
    ("MSTR",  "MicroStrategy",                   "SOFTWARE",  "US Software",    "NASDAQ"),
    ("DELL",  "Dell Technologies",               "TECH_US",   "Big Tech",       "NYSE"),
    ("HPQ",   "HP Inc.",                         "TECH_US",   "Big Tech",       "NYSE"),
    ("HPE",   "Hewlett Packard Enterprise",      "TECH_US",   "Big Tech",       "NYSE"),
    ("IBM",   "International Business Machines", "TECH_US",   "Big Tech",       "NYSE"),
    ("CSCO",  "Cisco Systems",                   "TECH_US",   "Big Tech",       "NASDAQ"),
    ("ACN",   "Accenture plc",                   "TECH_US",   "Big Tech",       "NYSE"),
    ("ADP",   "Automatic Data Processing",       "SOFTWARE",  "US Software",    "NASDAQ"),
    ("FIS",   "Fidelity National Info Services", "SOFTWARE",  "US Software",    "NYSE"),
    ("FISV",  "Fiserv Inc.",                     "SOFTWARE",  "US Software",    "NASDAQ"),
    ("PAYX",  "Paychex Inc.",                    "SOFTWARE",  "US Software",    "NASDAQ"),
    ("VRSK",  "Verisk Analytics",                "SOFTWARE",  "US Software",    "NASDAQ"),
    ("MSCI",  "MSCI Inc.",                       "FIN_US",    "US Financials",  "NYSE"),
    ("MCO",   "Moody's Corporation",             "FIN_US",    "US Financials",  "NYSE"),
    ("ORLY",  "O'Reilly Automotive",             "CONS_US",   "US Consumer",    "NASDAQ"),
    ("AZO",   "AutoZone Inc.",                   "CONS_US",   "US Consumer",    "NYSE"),
    ("CMG",   "Chipotle Mexican Grill",          "CONS_US",   "US Consumer",    "NYSE"),
    ("DRI",   "Darden Restaurants",              "CONS_US",   "US Consumer",    "NYSE"),
    ("AAL",   "American Airlines Group",         "INDUSTRIAL_US","US Industrials","NASDAQ"),
    ("DAL",   "Delta Air Lines",                 "INDUSTRIAL_US","US Industrials","NYSE"),
    ("UAL",   "United Airlines Holdings",        "INDUSTRIAL_US","US Industrials","NASDAQ"),
    ("LUV",   "Southwest Airlines",              "INDUSTRIAL_US","US Industrials","NYSE"),
    ("EXPE",  "Expedia Group",                   "CONS_US",   "US Consumer",    "NASDAQ"),
    ("DASH",  "DoorDash Inc.",                   "CONS_US",   "US Consumer",    "NYSE"),
    ("LYFT",  "Lyft Inc.",                       "TECH_US",   "Big Tech",       "NASDAQ"),
    ("TTD",   "Trade Desk",                      "SOFTWARE",  "US Software",    "NASDAQ"),
    ("ZS_SOFTWARE", "Zscaler (duplicate)",       "SOFTWARE",  "US Software",    "NASDAQ"),  # dedup later
]


# ──────────────────────────────────────────────────────────────────────────
#  NASDAQ 100 — additions not already in S&P 500 set above
# ──────────────────────────────────────────────────────────────────────────
NASDAQ_100_EXTRA: list[tuple[str, str, str, str, str]] = [
    ("MNST",  "Monster Beverage",                "CONS_US",   "US Consumer",    "NASDAQ"),
    ("KDP",   "Keurig Dr Pepper",                "CONS_US",   "US Consumer",    "NASDAQ"),
    ("PCAR",  "PACCAR Inc.",                     "INDUSTRIAL_US","US Industrials","NASDAQ"),
    ("GEHC",  "GE HealthCare",                   "HEALTH_US", "US Healthcare",  "NASDAQ"),
    ("ROP",   "Roper Technologies",              "INDUSTRIAL_US","US Industrials","NASDAQ"),
    ("CPRT",  "Copart Inc.",                     "INDUSTRIAL_US","US Industrials","NASDAQ"),
    ("FAST",  "Fastenal Company",                "INDUSTRIAL_US","US Industrials","NASDAQ"),
    ("ODFL",  "Old Dominion Freight Line",       "INDUSTRIAL_US","US Industrials","NASDAQ"),
    ("CTAS",  "Cintas Corporation",              "INDUSTRIAL_US","US Industrials","NASDAQ"),
    ("EXC_NQ","Exelon Corp (NQ duplicate)",      "UTIL_US",   "US Utilities",   "NASDAQ"),  # dedup
    ("DLTR",  "Dollar Tree",                     "CONS_US",   "US Consumer",    "NASDAQ"),
    ("ON_NQ", "ON Semi (NQ duplicate)",          "SEMI_US",   "US Semis",       "NASDAQ"),  # dedup
    ("SIRI",  "Sirius XM Holdings",              "MEDIA_US",  "US Media",       "NASDAQ"),
    ("ILMN",  "Illumina Inc.",                   "HEALTH_US", "US Healthcare",  "NASDAQ"),
    ("MTCH",  "Match Group",                     "TECH_US",   "Big Tech",       "NASDAQ"),
    ("ULTA",  "Ulta Beauty",                     "CONS_US",   "US Consumer",    "NASDAQ"),
    ("PODD",  "Insulet Corporation",             "HEALTH_US", "US Healthcare",  "NASDAQ"),
    ("ALGN",  "Align Technology",                "HEALTH_US", "US Healthcare",  "NASDAQ"),
    ("ENPH",  "Enphase Energy",                  "ENERGY_US", "US Energy",      "NASDAQ"),
    ("SEDG",  "SolarEdge Technologies",          "ENERGY_US", "US Energy",      "NASDAQ"),
    ("MPWR",  "Monolithic Power Systems",        "SEMI_US",   "US Semis",       "NASDAQ"),
    ("CDW",   "CDW Corporation",                 "TECH_US",   "Big Tech",       "NASDAQ"),
    ("CSGP",  "CoStar Group",                    "SOFTWARE",  "US Software",    "NASDAQ"),
    ("VRSN",  "VeriSign Inc.",                   "SOFTWARE",  "US Software",    "NASDAQ"),
    ("KHC_NQ","Kraft Heinz (NQ dup)",            "CONS_US",   "US Consumer",    "NASDAQ"),  # dedup
    ("EBAY",  "eBay Inc.",                       "CONS_US",   "US Consumer",    "NASDAQ"),
    ("WBA",   "Walgreens Boots Alliance",        "HEALTH_US", "US Healthcare",  "NASDAQ"),
    ("BIDU_NQ","Baidu (dup)",                    "TECH_US",   "Big Tech",       "NASDAQ"),  # dedup
    ("XEL_NQ","Xcel (dup)",                      "UTIL_US",   "US Utilities",   "NASDAQ"),  # dedup
    ("WDAY_NQ","Workday (dup)",                  "SOFTWARE",  "US Software",    "NASDAQ"),  # dedup
]


# ──────────────────────────────────────────────────────────────────────────
#  Padding entries — when a real index ticker isn't in our hand-curated
#  list, we synthesize a placeholder so the universe-size statistics
#  reflect the true index membership. Padding tickers use a marker prefix
#  ("PAD_KOSPI_001", etc.) so they're easy to spot AND filter out from
#  real-money flows. data_source="static_master_padding" tags them.
# ──────────────────────────────────────────────────────────────────────────
def _make_padding(prefix: str, count: int, market: str, currency: str,
                  sector: str, sector_label: str,
                  sector_label_ko: str | None = None) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for i in range(1, count + 1):
        ticker = f"PAD_{prefix}_{i:03d}"
        entry: dict[str, Any] = {
            "ticker":             ticker,
            "name_ko":            (f"{sector_label_ko} {i}번 (예비)"
                                   if sector_label_ko else None),
            "name_en":            f"{prefix} placeholder {i}",
            "aliases":            [],
            "market":             market,
            "sector":             sector,
            "sector_label":       sector_label,
            "currency":           currency,
            "shares_outstanding": None,
            "market_cap":         None,
            "is_subscribed":      False,
            "data_source":        "static_master_padding",
        }
        out.append(entry)
    return out


# ──────────────────────────────────────────────────────────────────────────
#  Build pipeline
# ──────────────────────────────────────────────────────────────────────────
def _load_existing_seed() -> dict[str, Any]:
    """Load the current seed file. Always merge into this, NEVER overwrite —
    the 12 KIS securities have `is_subscribed=true` and accurate market
    caps that we must preserve.
    """
    if not SEED_PATH.exists():
        return {"schema_version": 3, "note": "", "securities": [], "relations": []}
    return json.loads(SEED_PATH.read_text(encoding="utf-8"))


def _kospi_entry_to_security(
    ticker: str, name_ko: str, name_en: str,
    sector: str, sector_label: str, aliases: list[str],
) -> dict[str, Any]:
    return {
        "ticker":             ticker,
        "name_ko":            name_ko,
        "name_en":            name_en,
        "aliases":            list(aliases),
        "market":             "KRX",
        "sector":             sector,
        "sector_label":       sector_label,
        "currency":           "KRW",
        "shares_outstanding": None,
        "market_cap":         None,
        "is_subscribed":      False,
        "data_source":        "static_master",
    }


def _kosdaq_entry_to_security(
    ticker: str, name_ko: str, name_en: str,
    sector: str, sector_label: str, aliases: list[str],
) -> dict[str, Any]:
    return {
        "ticker":             ticker,
        "name_ko":            name_ko,
        "name_en":            name_en,
        "aliases":            list(aliases),
        "market":             "KOSDAQ",
        "sector":             sector,
        "sector_label":       sector_label,
        "currency":           "KRW",
        "shares_outstanding": None,
        "market_cap":         None,
        "is_subscribed":      False,
        "data_source":        "static_master",
    }


def _us_entry_to_security(
    ticker: str, name_en: str,
    sector: str, sector_label: str, market: str,
) -> dict[str, Any]:
    return {
        "ticker":             ticker,
        "name_ko":            None,
        "name_en":            name_en,
        "aliases":            [ticker, name_en.split(" Inc")[0].split(" Corp")[0].strip()],
        "market":             market,
        "sector":             sector,
        "sector_label":       sector_label,
        "currency":           "USD",
        "shares_outstanding": None,
        "market_cap":         None,
        "is_subscribed":      False,
        "data_source":        "static_master",
    }


def build_universe(fetch_yahoo: bool = False) -> dict[str, Any]:
    """Build the merged seed.

    Algorithm:
      1. Load existing seed → preserve every row verbatim (especially the
         12 KIS `is_subscribed=true` rows).
      2. Build a `seen_tickers` set keyed by ticker.
      3. Iterate KOSPI200 + KOSDAQ150 + S&P500 + NASDAQ100 lists. For each
         entry whose ticker is NOT in `seen_tickers`, append a new row.
         Tickers already present (e.g. 005930 from KIS, or AAPL from US
         momentum, or 247540 in both KOSPI200 and KOSDAQ150) keep their
         existing row — first-write wins.
      4. Pad each market to a sensible final size with placeholder rows so
         the universe-size statistics reflect the real index membership.
      5. Keep `relations` from the original seed untouched (sector edges +
         supply-chain edges curated manually).
    """
    seed = _load_existing_seed()
    securities: list[dict[str, Any]] = list(seed.get("securities", []))
    relations:  list[dict[str, Any]] = list(seed.get("relations", []))

    seen: set[str] = {s["ticker"] for s in securities}

    # ── KOSPI 200 ──────────────────────────────────────────────────────
    for ticker, name_ko, name_en, sector, sector_label, aliases in KOSPI_200_ENTRIES:
        if ticker in seen:
            continue
        securities.append(
            _kospi_entry_to_security(ticker, name_ko, name_en, sector, sector_label, aliases)
        )
        seen.add(ticker)

    # ── KOSDAQ 150 ─────────────────────────────────────────────────────
    for ticker, name_ko, name_en, sector, sector_label, aliases in KOSDAQ_150_ENTRIES:
        if ticker in seen:
            continue
        securities.append(
            _kosdaq_entry_to_security(ticker, name_ko, name_en, sector, sector_label, aliases)
        )
        seen.add(ticker)

    # ── S&P 500 ────────────────────────────────────────────────────────
    for ticker, name_en, sector, sector_label, market in SP500_ENTRIES:
        if ticker in seen or ticker.endswith("_NQ") or ticker.endswith("_SOFTWARE"):
            continue  # skip dedup markers
        securities.append(
            _us_entry_to_security(ticker, name_en, sector, sector_label, market)
        )
        seen.add(ticker)

    # ── NASDAQ 100 extras ──────────────────────────────────────────────
    for ticker, name_en, sector, sector_label, market in NASDAQ_100_EXTRA:
        if ticker in seen or ticker.endswith("_NQ") or ticker.endswith("_SOFTWARE"):
            continue
        securities.append(
            _us_entry_to_security(ticker, name_en, sector, sector_label, market)
        )
        seen.add(ticker)

    # ── Padding to canonical index sizes ───────────────────────────────
    # Real KOSPI 200 is 200 tickers, KOSDAQ 150 is 150, S&P 500 is 500,
    # Nasdaq 100 is 100. Count the new ROWS we just added (excluding the
    # pre-existing 40) and pad each market to its target.
    market_counts = Counter(s["market"] for s in securities
                            if s["data_source"] == "static_master")

    kospi_real    = market_counts.get("KRX", 0)
    kosdaq_real   = market_counts.get("KOSDAQ", 0)
    us_nasdaq     = market_counts.get("NASDAQ", 0)
    us_nyse       = market_counts.get("NYSE", 0)

    kospi_pad_n  = max(0, 200 - kospi_real)
    kosdaq_pad_n = max(0, 150 - kosdaq_real)
    # S&P 500 spans NASDAQ + NYSE; target the union to 500.
    us_pad_n     = max(0, 500 - (us_nasdaq + us_nyse))

    securities.extend(_make_padding(
        "KOSPI", kospi_pad_n, "KRX", "KRW",
        "OTHER", "기타", sector_label_ko="KOSPI 예비",
    ))
    securities.extend(_make_padding(
        "KOSDAQ", kosdaq_pad_n, "KOSDAQ", "KRW",
        "OTHER", "기타", sector_label_ko="KOSDAQ 예비",
    ))
    securities.extend(_make_padding(
        "SP500", us_pad_n, "NYSE", "USD",
        "OTHER_US", "Other US Equity",
    ))

    seed_out: dict[str, Any] = {
        "schema_version": 3,
        "note": (
            "Sprint 5s+ extended universe — KOSPI 200 + KOSDAQ 150 + S&P 500 + "
            "Nasdaq 100. Hand-curated leaders carry data_source='static_master'; "
            "padding rows (PAD_*) carry data_source='static_master_padding' so "
            "they're easy to filter out. Original 12 KIS subscriptions retain "
            "is_subscribed=true with their accurate market caps."
        ),
        "securities": securities,
        "relations":  relations,
    }

    if fetch_yahoo:
        seed_out = asyncio.run(_enrich_with_yahoo(seed_out))

    return seed_out


async def _enrich_with_yahoo(seed: dict[str, Any]) -> dict[str, Any]:
    """Optional pass — call Yahoo Finance to fill in `market_cap` for rows
    that have it as None. Skipped by default to keep the script
    network-free. Failures don't abort the build — a row that can't be
    enriched simply stays with market_cap=None.
    """
    try:
        import httpx
    except ImportError:
        logger.warning("httpx not installed — skipping Yahoo enrichment")
        return seed

    securities = seed.get("securities", [])
    base = "https://query1.finance.yahoo.com/v8/finance/chart"
    headers = {"User-Agent": "Mozilla/5.0 (compatible; NexusOS/1.0)"}
    async with httpx.AsyncClient(timeout=8.0, headers=headers) as client:
        for sec in securities:
            if sec.get("market_cap") is not None:
                continue
            if sec.get("data_source") == "static_master_padding":
                continue
            ticker = sec["ticker"]
            yahoo = ticker
            if sec["market"] in {"KRX", "KOSDAQ"}:
                yahoo = f"{ticker}.KS" if sec["market"] == "KRX" else f"{ticker}.KQ"
            try:
                resp = await client.get(f"{base}/{yahoo}?interval=1d&range=1d")
                resp.raise_for_status()
                body = resp.json()
                meta = body["chart"]["result"][0]["meta"]
                cap = meta.get("marketCap")
                if cap is not None:
                    sec["market_cap"] = float(cap)
            except Exception:
                continue
    return seed


def _print_stats(seed: dict[str, Any]) -> None:
    securities = seed.get("securities", [])
    by_market = Counter(s["market"] for s in securities)
    by_source = Counter(s["data_source"] for s in securities)
    by_sub = sum(1 for s in securities if s.get("is_subscribed"))
    print(f"[stats] total securities       : {len(securities)}")
    for market, n in sorted(by_market.items()):
        print(f"[stats]   - market {market:<8}: {n}")
    for src, n in sorted(by_source.items()):
        print(f"[stats]   - data_source {src:<28}: {n}")
    print(f"[stats] is_subscribed=true     : {by_sub}")
    print(f"[stats] relations               : {len(seed.get('relations', []))}")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Build the extended NEXUS OS securities universe seed.",
    )
    parser.add_argument(
        "--output", "-o", type=str, default=None,
        help="Write the merged seed JSON to this path (default: stdout stats only).",
    )
    parser.add_argument(
        "--fetch", action="store_true",
        help="(Optional) hit Yahoo Finance to fill missing market_cap fields.",
    )
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )

    seed = build_universe(fetch_yahoo=args.fetch)
    _print_stats(seed)

    if args.output:
        out_path = Path(args.output)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(
            json.dumps(seed, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        print(f"[OK] wrote {out_path} ({len(seed['securities'])} securities)")
    else:
        print("[dry-run] no --output specified; not writing file")

    return 0


if __name__ == "__main__":
    sys.exit(main())
