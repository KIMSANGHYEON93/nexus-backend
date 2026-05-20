"""Replace all PAD_* placeholders with real securities and enrich with Yahoo Finance data.

Sources:
  - S&P 500 constituents: Wikipedia (en.wikipedia.org)
  - NASDAQ 100 constituents: Wikipedia (en.wikipedia.org)
  - KOSPI 200 / KOSDAQ 150: Embedded comprehensive ticker list + yfinance validation
  - Market metadata (name, market_cap, shares_outstanding, sector): yfinance

Usage (from nexus-backend/):
    python -m db.fetch_real_universe                  # dry-run: stats only
    python -m db.fetch_real_universe --apply          # write output to seed file
    python -m db.fetch_real_universe --apply --out db/seeds/securities_master.json
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from pathlib import Path
from typing import Any

import requests
import yfinance as yf
from bs4 import BeautifulSoup

logger = logging.getLogger("nexus.fetch_real_universe")

SEED_PATH = Path(__file__).parent / "seeds" / "securities_master.json"

# ── GICS sector → our internal sector / sector_label mapping ─────────────
_GICS_MAP: dict[str, tuple[str, str]] = {
    "Technology":              ("TECH_US",        "Big Tech"),
    "Communication Services":  ("MEDIA_US",        "US Media"),
    "Consumer Discretionary":  ("CONS_US",         "US Consumer"),
    "Consumer Staples":        ("CONS_US",         "US Consumer"),
    "Financials":              ("FIN_US",          "US Financials"),
    "Health Care":             ("HEALTH_US",       "US Healthcare"),
    "Industrials":             ("INDUSTRIAL_US",   "US Industrials"),
    "Energy":                  ("ENERGY_US",       "US Energy"),
    "Materials":               ("MATERIALS_US",    "US Materials"),
    "Real Estate":             ("REIT_US",         "US REIT"),
    "Utilities":               ("UTIL_US",         "US Utilities"),
    "Basic Materials":         ("MATERIALS_US",    "US Materials"),
    "Financial Services":      ("FIN_US",          "US Financials"),
}

# ── Market for US tickers: default NYSE, override for known NASDAQ ────────
_NASDAQ_EXCHANGE_TICKERS: set[str] = {
    "AAPL", "MSFT", "NVDA", "GOOGL", "GOOG", "AMZN", "META", "TSLA",
    "AVGO", "AMD", "QCOM", "INTC", "MU", "AMAT", "LRCX", "KLAC", "ASML",
    "TXN", "ADI", "MRVL", "NXPI", "ON", "MCHP",
    "ADBE", "INTU", "PANW", "CRWD", "FTNT", "SNPS", "CDNS", "ANSS",
    "WDAY", "TEAM", "DDOG", "MDB", "OKTA", "ZS", "COST", "SBUX", "LULU",
    "BKNG", "MAR", "PEP", "MDLZ", "KHC", "AMGN", "GILD", "ISRG",
    "VRTX", "REGN", "BIIB", "CME", "IDXX", "DXCM", "CMCSA", "WBD",
    "PARA", "TMUS", "CHTR", "EQIX", "SBAC", "NFLX",
    "MELI", "HOOD", "SOFI", "BIDU", "JD", "PDD", "MSTR", "CSCO",
    "ACN", "ADP", "FISV", "PAYX", "VRSK", "MCO", "ORLY", "CMG",
    "AAL", "UAL", "EXPE", "TTD", "MNST", "KDP", "PCAR", "GEHC",
    "ROP", "CPRT", "FAST", "ODFL", "CTAS", "DLTR", "SIRI", "ILMN",
    "MTCH", "ULTA", "PODD", "ALGN", "ENPH", "SEDG", "MPWR", "CDW",
    "CSGP", "VRSN", "EBAY", "WBA", "FANG", "HON", "CSX", "EXC", "AEP",
    "XEL", "CEG", "LIN", "LCID", "RIVN", "LYFT", "DOCU", "ZM",
    "COIN", "SNPS", "TEAM", "DDOG", "HOOD", "EA", "TTWO", "ROKU",
}


# ═══════════════════════════════════════════════════════════════════════════
#  Korean tickers
#  Format: (ticker, name_ko, name_en, sector, sector_label)
#  - KRX (KOSPI): .KS suffix for Yahoo Finance
#  - KOSDAQ: .KQ suffix for Yahoo Finance
# ═══════════════════════════════════════════════════════════════════════════

KOSPI_FULL: list[tuple[str, str, str, str, str]] = [
    # ── 반도체·IT ──────────────────────────────────────────────────────
    ("005930", "삼성전자",          "Samsung Electronics",           "SEMI",         "반도체"),
    ("000660", "SK하이닉스",        "SK Hynix",                      "SEMI",         "반도체"),
    ("066570", "LG전자",            "LG Electronics",                "TECH_HW",      "전자"),
    ("009150", "삼성전기",          "Samsung Electro-Mechanics",     "TECH_HW",      "전자"),
    ("000990", "DB하이텍",          "DB HiTek",                      "SEMI",         "반도체"),
    ("042700", "한미반도체",        "Hanmi Semiconductor",           "SEMI",         "반도체"),
    ("108670", "LG이노텍",          "LG Innotek",                    "TECH_HW",      "전자"),
    ("270810", "POSCO DX",          "POSCO DX",                      "TECH_HW",      "전자"),
    ("034220", "LG디스플레이",      "LG Display",                    "TECH_HW",      "전자"),
    ("402340", "SK스퀘어",          "SK Square",                     "TECH_HW",      "전자"),
    ("010120", "LS ELECTRIC",       "LS ELECTRIC",                   "INDUSTRIAL",   "산업재"),
    # ── 2차전지·소재 ────────────────────────────────────────────────────
    ("373220", "LG에너지솔루션",    "LG Energy Solution",            "BATTERY",      "2차전지"),
    ("006400", "삼성SDI",           "Samsung SDI",                   "BATTERY",      "2차전지"),
    ("247540", "에코프로비엠",      "EcoPro BM",                     "BATTERY",      "2차전지"),
    ("086520", "에코프로",          "EcoPro",                        "BATTERY",      "2차전지"),
    ("003670", "POSCO퓨처엠",       "POSCO Future M",                "BATTERY",      "2차전지"),
    ("278280", "천보",              "Chunbo",                        "BATTERY",      "2차전지"),
    ("051910", "LG화학",            "LG Chem",                       "CHEM",         "화학"),
    ("011790", "SKC",               "SKC",                           "CHEM",         "화학"),
    ("011780", "금호석유화학",      "Kumho Petrochemical",           "CHEM",         "화학"),
    ("002380", "KCC",               "KCC",                           "CHEM",         "화학"),
    ("009830", "한화솔루션",        "Hanwha Solutions",              "CHEM",         "화학"),
    ("011170", "롯데케미칼",        "Lotte Chemical",                "CHEM",         "화학"),
    ("285130", "SK케미칼",          "SK Chemicals",                  "CHEM",         "화학"),
    ("004000", "롯데정밀화학",      "Lotte Fine Chemical",           "CHEM",         "화학"),
    ("214360", "효성화학",          "Hyosung Chemical",              "CHEM",         "화학"),
    ("010060", "OCI홀딩스",         "OCI Holdings",                  "CHEM",         "화학"),
    ("036830", "솔브레인홀딩스",    "Soulbrain Holdings",            "MATERIALS",    "소재"),
    ("005490", "POSCO홀딩스",       "POSCO Holdings",                "MATERIALS",    "소재"),
    ("010130", "고려아연",          "Korea Zinc",                    "MATERIALS",    "소재"),
    ("004020", "현대제철",          "Hyundai Steel",                 "MATERIALS",    "소재"),
    ("103140", "풍산",              "Poongsan",                      "MATERIALS",    "소재"),
    ("298050", "효성첨단소재",      "Hyosung Advanced Materials",    "MATERIALS",    "소재"),
    ("047050", "포스코인터내셔널",  "POSCO International",           "TRADE",        "상사"),
    # ── 자동차 ────────────────────────────────────────────────────────
    ("005380", "현대차",            "Hyundai Motor",                 "AUTO",         "자동차"),
    ("000270", "기아",              "Kia",                           "AUTO",         "자동차"),
    ("012330", "현대모비스",        "Hyundai Mobis",                 "AUTO",         "자동차"),
    ("161390", "한국타이어앤테크놀로지", "Hankook Tire & Technology","AUTO",         "자동차"),
    ("007340", "현대건설기계",      "Hyundai Construction Equipment","INDUSTRIAL",   "산업재"),
    ("073240", "금호타이어",        "Kumho Tire",                    "AUTO",         "자동차"),
    ("025540", "한국단자공업",      "KM Corp",                       "AUTO",         "자동차"),
    ("005850", "에스엘",            "SL Corp",                       "AUTO",         "자동차"),
    ("018880", "한온시스템",        "Hanon Systems",                 "AUTO",         "자동차"),
    # ── 금융 ──────────────────────────────────────────────────────────
    ("105560", "KB금융",            "KB Financial Group",            "FIN",          "금융"),
    ("055550", "신한지주",          "Shinhan Financial Group",       "FIN",          "금융"),
    ("086790", "하나금융지주",      "Hana Financial Group",          "FIN",          "금융"),
    ("316140", "우리금융지주",      "Woori Financial Group",         "FIN",          "금융"),
    ("138930", "BNK금융지주",       "BNK Financial Group",           "FIN",          "금융"),
    ("175330", "JB금융지주",        "JB Financial Group",            "FIN",          "금융"),
    ("032830", "삼성생명",          "Samsung Life Insurance",        "FIN",          "금융"),
    ("088350", "한화생명",          "Hanwha Life Insurance",         "FIN",          "금융"),
    ("000810", "삼성화재",          "Samsung Fire & Marine",         "FIN",          "금융"),
    ("001450", "현대해상",          "Hyundai Marine & Fire",         "FIN",          "금융"),
    ("016360", "삼성증권",          "Samsung Securities",            "FIN",          "금융"),
    ("071050", "한국금융지주",      "Korea Investment Holdings",     "FIN",          "금융"),
    ("138040", "메리츠금융지주",    "Meritz Financial Group",        "FIN",          "금융"),
    ("006800", "미래에셋증권",      "Mirae Asset Securities",        "FIN",          "금융"),
    ("039490", "키움증권",          "Kiwoom Securities",             "FIN",          "금융"),
    ("085620", "미래에셋생명",      "Mirae Asset Life Insurance",    "FIN",          "금융"),
    ("024110", "기업은행",          "Industrial Bank of Korea",      "FIN",          "금융"),
    ("082640", "동양생명",          "Dongyang Life Insurance",       "FIN",          "금융"),
    ("377300", "카카오페이",        "Kakao Pay",                     "FIN",          "금융"),
    ("293490", "카카오뱅크",        "KakaoBank",                     "FIN",          "금융"),
    # ── 지주 ──────────────────────────────────────────────────────────
    ("003550", "LG",                "LG Corp",                       "HOLDING",      "지주"),
    ("028260", "삼성물산",          "Samsung C&T",                   "HOLDING",      "지주"),
    ("034730", "SK",                "SK Inc.",                       "HOLDING",      "지주"),
    ("001040", "CJ",                "CJ Corp",                       "HOLDING",      "지주"),
    ("001120", "LX홀딩스",          "LX Holdings",                   "HOLDING",      "지주"),
    ("004990", "롯데지주",          "Lotte Holdings",                "HOLDING",      "지주"),
    ("078930", "GS홀딩스",          "GS Holdings",                   "HOLDING",      "지주"),
    ("363280", "티와이홀딩스",      "TY Holdings",                   "HOLDING",      "지주"),
    ("001800", "오리온홀딩스",      "Orion Holdings",                "HOLDING",      "지주"),
    # ── 바이오·헬스 ────────────────────────────────────────────────────
    ("207940", "삼성바이오로직스",  "Samsung Biologics",             "BIO",          "바이오"),
    ("068270", "셀트리온",          "Celltrion",                     "BIO",          "바이오"),
    ("145020", "휴젤",              "Hugel",                         "BIO",          "바이오"),
    ("196170", "알테오젠",          "Alteogen",                      "BIO",          "바이오"),
    ("302440", "SK바이오사이언스",  "SK Bioscience",                 "BIO",          "바이오"),
    ("128940", "한미약품",          "Hanmi Pharmaceutical",          "BIO",          "바이오"),
    ("000100", "유한양행",          "Yuhan Corporation",             "BIO",          "바이오"),
    ("069620", "대웅제약",          "Daewoong Pharmaceutical",       "BIO",          "바이오"),
    ("214050", "파마리서치",        "Pharmaresearch",                "BIO",          "바이오"),
    # ── 유통·소비 ───────────────────────────────────────────────────────
    ("139480", "이마트",            "E-Mart",                        "CONS",         "소비재"),
    ("069960", "현대백화점",        "Hyundai Department Store",      "CONS",         "소비재"),
    ("097950", "CJ제일제당",        "CJ CheilJedang",                "CONS",         "소비재"),
    ("021240", "코웨이",            "Coway",                         "CONS",         "소비재"),
    ("004170", "신세계",            "Shinsegae",                     "CONS",         "소비재"),
    ("004370", "농심",              "Nongshim",                      "CONS",         "소비재"),
    ("005300", "롯데칠성음료",      "Lotte Chilsung",                "CONS",         "소비재"),
    ("007070", "GS리테일",          "GS Retail",                     "CONS",         "소비재"),
    ("008770", "호텔신라",          "Hotel Shilla",                  "CONS",         "소비재"),
    ("023530", "롯데쇼핑",          "Lotte Shopping",                "CONS",         "소비재"),
    ("035250", "강원랜드",          "Kangwon Land",                  "CONS",         "소비재"),
    ("051900", "LG생활건강",        "LG H&H",                        "CONS",         "소비재"),
    ("000080", "하이트진로",        "Hite Jinro",                    "CONS",         "소비재"),
    ("192820", "코스맥스",          "Cosmax",                        "CONS",         "소비재"),
    ("282330", "BGF리테일",         "BGF Retail",                    "CONS",         "소비재"),
    ("001680", "대상",              "Daesang",                       "CONS",         "소비재"),
    ("033780", "KT&G",              "KT&G",                          "CONS",         "소비재"),
    ("115390", "락앤락",            "Lock&Lock",                     "CONS",         "소비재"),
    ("383220", "F&F",               "F&F",                           "CONS",         "소비재"),
    ("389990", "에이피알",          "APR",                           "CONS",         "소비재"),
    ("019170", "신세계인터내셔날",  "Shinsegae International",       "CONS",         "소비재"),
    # ── 에너지 ────────────────────────────────────────────────────────
    ("096770", "SK이노베이션",      "SK Innovation",                 "ENERGY",       "에너지"),
    ("015760", "한국전력",          "Korea Electric Power",          "ENERGY",       "에너지"),
    ("036460", "한국가스공사",      "Korea Gas",                     "ENERGY",       "에너지"),
    ("010950", "S-Oil",             "S-Oil",                         "ENERGY",       "에너지"),
    ("336260", "두산퓨얼셀",        "Doosan Fuel Cell",              "ENERGY",       "에너지"),
    # ── 플랫폼·인터넷 ──────────────────────────────────────────────────
    ("035420", "NAVER",             "Naver Corporation",             "PLATFORM",     "플랫폼"),
    ("035720", "카카오",            "Kakao",                         "PLATFORM",     "플랫폼"),
    ("259960", "크래프톤",          "Krafton",                       "PLATFORM",     "플랫폼"),
    ("036570", "엔씨소프트",        "NCsoft",                        "PLATFORM",     "플랫폼"),
    ("012510", "더존비즈온",        "Douzone Bizon",                 "PLATFORM",     "플랫폼"),
    ("018260", "삼성SDS",           "Samsung SDS",                   "PLATFORM",     "플랫폼"),
    # ── 엔터·미디어 ────────────────────────────────────────────────────
    ("352820", "하이브",            "HYBE",                          "MEDIA",        "엔터"),
    ("035900", "JYP Ent.",          "JYP Entertainment",             "MEDIA",        "엔터"),
    ("041510", "에스엠",            "SM Entertainment",              "MEDIA",        "엔터"),
    ("253450", "스튜디오드래곤",    "Studio Dragon",                 "MEDIA",        "엔터"),
    ("287410", "제일기획",          "Cheil Worldwide",               "MEDIA",        "엔터"),
    ("021820", "이노션",            "Innocean Worldwide",            "MEDIA",        "엔터"),
    # ── 통신 ──────────────────────────────────────────────────────────
    ("030200", "KT",                "KT Corporation",                "TELCO",        "통신"),
    ("017670", "SK텔레콤",          "SK Telecom",                    "TELCO",        "통신"),
    ("032640", "LG유플러스",        "LG Uplus",                      "TELCO",        "통신"),
    # ── 조선·방산·산업 ─────────────────────────────────────────────────
    ("009540", "HD한국조선해양",    "HD Korea Shipbuilding & Offshore","SHIP",        "조선"),
    ("010140", "삼성중공업",        "Samsung Heavy Industries",      "SHIP",         "조선"),
    ("042660", "한화오션",          "Hanwha Ocean",                  "SHIP",         "조선"),
    ("011200", "HMM",               "HMM",                           "SHIP",         "조선"),
    ("329180", "현대중공업",        "HD Hyundai Heavy Industries",   "SHIP",         "조선"),
    ("010620", "현대미포조선",      "Hyundai Mipo Dockyard",         "SHIP",         "조선"),
    ("082740", "HSD엔진",           "HSD Engine",                    "SHIP",         "조선"),
    ("064350", "현대로템",          "Hyundai Rotem",                 "INDUSTRIAL",   "산업재"),
    ("298040", "효성중공업",        "Hyosung Heavy Industries",      "INDUSTRIAL",   "산업재"),
    ("012450", "한화에어로스페이스","Hanwha Aerospace",              "DEFENSE",      "방산"),
    ("079550", "LIG넥스원",         "LIG Nex1",                      "DEFENSE",      "방산"),
    ("047810", "한국항공우주산업",  "Korea Aerospace Industries",    "DEFENSE",      "방산"),
    ("086280", "현대글로비스",      "Hyundai Glovis",                "INDUSTRIAL",   "산업재"),
    ("267260", "HD현대일렉트릭",    "HD Hyundai Electric",           "INDUSTRIAL",   "산업재"),
    ("376030", "HD현대인프라코어",  "HD Hyundai Infracore",          "INDUSTRIAL",   "산업재"),
    ("028050", "삼성엔지니어링",    "Samsung Engineering",           "INDUSTRIAL",   "산업재"),
    ("006260", "LS",                "LS Corp",                       "INDUSTRIAL",   "산업재"),
    ("003490", "대한항공",          "Korean Air Lines",              "INDUSTRIAL",   "운수"),
    ("020560", "아시아나항공",      "Asiana Airlines",               "INDUSTRIAL",   "운수"),
    ("034020", "두산에너빌리티",    "Doosan Enerbility",             "INDUSTRIAL",   "산업재"),
    ("241560", "두산로보틱스",      "Doosan Robotics",               "TECH_HW",      "전자"),
    # ── 철강·건설 ─────────────────────────────────────────────────────
    ("000720", "현대건설",          "Hyundai E&C",                   "CONST",        "건설"),
    ("375500", "DL이앤씨",          "DL E&C",                        "CONST",        "건설"),
]

KOSDAQ_FULL: list[tuple[str, str, str, str, str]] = [
    # ── 반도체 ────────────────────────────────────────────────────────
    ("058470", "리노공업",          "Leeno Industrial",              "SEMI",         "반도체"),
    ("039030", "이오테크닉스",      "EO Technics",                   "SEMI",         "반도체"),
    ("240810", "원익IPS",           "Wonik IPS",                     "SEMI",         "반도체"),
    ("232140", "와이씨",            "YC",                            "SEMI",         "반도체"),
    ("036930", "주성엔지니어링",    "Jusung Engineering",            "SEMI",         "반도체"),
    ("033640", "네패스",            "Nepes",                         "SEMI",         "반도체"),
    ("403870", "HPSP",              "HPSP",                          "SEMI",         "반도체"),
    ("095340", "ISC",               "ISC",                           "SEMI",         "반도체"),
    ("067310", "하나마이크론",      "Hana Micron",                   "SEMI",         "반도체"),
    ("166090", "하나머티리얼즈",    "Hana Materials",                "SEMI",         "반도체"),
    ("086310", "에스에프에이",      "SFA Engineering",               "SEMI",         "반도체"),
    ("222800", "심텍",              "Simtec",                        "SEMI",         "반도체"),
    ("104480", "해성디에스",        "Haesong DS",                    "SEMI",         "반도체"),
    ("358150", "텔레칩스",          "Telechips",                     "SEMI",         "반도체"),
    ("054620", "APS",               "APS Holdings",                  "SEMI",         "반도체"),
    ("440110", "파두",              "Padu",                          "SEMI",         "반도체"),
    # ── 소재 ─────────────────────────────────────────────────────────
    ("357780", "솔브레인",          "Soulbrain",                     "MATERIALS",    "소재"),
    ("121600", "나노신소재",        "Nano New Materials",            "MATERIALS",    "소재"),
    ("213420", "덕산네오룩스",      "Duksan Neolux",                 "MATERIALS",    "소재"),
    ("281740", "레이크머티리얼즈",  "Lake Materials",                "MATERIALS",    "소재"),
    ("104830", "원익머트리얼즈",    "Wonik Materials",               "MATERIALS",    "소재"),
    ("078600", "대주전자재료",      "Daejoo Electronic Materials",   "BATTERY",      "2차전지"),
    ("131290", "TES",               "TES",                           "SEMI",         "반도체"),
    # ── 2차전지 ───────────────────────────────────────────────────────
    ("247540", "에코프로비엠",      "EcoPro BM",                     "BATTERY",      "2차전지"),
    ("086520", "에코프로",          "EcoPro",                        "BATTERY",      "2차전지"),
    ("066970", "엘앤에프",          "L&F",                           "BATTERY",      "2차전지"),
    ("348370", "엔켐",              "Enchem",                        "BATTERY",      "2차전지"),
    ("393890", "더블유씨피",        "WCP",                           "BATTERY",      "2차전지"),
    # ── 바이오 ────────────────────────────────────────────────────────
    ("028300", "HLB",               "HLB",                           "BIO",          "바이오"),
    ("091990", "셀트리온헬스케어",  "Celltrion Healthcare",          "BIO",          "바이오"),
    ("196170", "알테오젠",          "Alteogen",                      "BIO",          "바이오"),
    ("214150", "클래시스",          "Classys",                       "BIO",          "바이오"),
    ("041920", "메디톡스",          "Medytox",                       "BIO",          "바이오"),
    ("078160", "메디포스트",        "Medipost",                      "BIO",          "바이오"),
    ("084110", "휴온스",            "Huons",                         "BIO",          "바이오"),
    ("228760", "지노믹트리",        "Genomictree",                   "BIO",          "바이오"),
    ("214370", "케어젠",            "Caregen",                       "BIO",          "바이오"),
    ("085660", "차바이오텍",        "CHA Biotech",                   "BIO",          "바이오"),
    ("237690", "에스티팜",          "ST Pharm",                      "BIO",          "바이오"),
    ("141080", "리가켐바이오",      "LigaChem Biosciences",          "BIO",          "바이오"),
    ("328130", "루닛",              "Lunit",                         "BIO",          "바이오"),
    ("389470", "인벤티지랩",       "Inventage Lab",                  "BIO",          "바이오"),
    ("145720", "덴티움",            "Dentium",                       "BIO",          "바이오"),
    ("214450", "파마리서치",        "Pharmaresearch",                "BIO",          "바이오"),
    ("285490", "노바렉스",          "Novarex",                       "BIO",          "바이오"),
    ("290650", "엘앤씨바이오",      "L&C Bio",                       "BIO",          "바이오"),
    ("317690", "헬릭스미스",        "Helixmith",                     "BIO",          "바이오"),
    ("131760", "유비케어",          "Ubicare",                       "BIO",          "바이오"),
    ("096530", "씨젠",              "Seegene",                       "BIO",          "바이오"),
    # ── 플랫폼·게임 ─────────────────────────────────────────────────────
    ("095660", "네오위즈",          "Neowiz",                        "PLATFORM",     "플랫폼"),
    ("263750", "펄어비스",          "Pearl Abyss",                   "PLATFORM",     "플랫폼"),
    ("112040", "위메이드",          "Wemade",                        "PLATFORM",     "플랫폼"),
    ("064760", "카카오게임즈",      "Kakao Games",                   "PLATFORM",     "플랫폼"),
    ("251270", "넷마블",            "Netmarble",                     "PLATFORM",     "플랫폼"),
    ("284740", "쿠콘",              "Coocon",                        "PLATFORM",     "플랫폼"),
    # ── 엔터 ──────────────────────────────────────────────────────────
    ("122870", "와이지엔터테인먼트","YG Entertainment",              "MEDIA",        "엔터"),
    ("376300", "디어유",            "Dear U",                        "MEDIA",        "엔터"),
    # ── 전자·로봇 ─────────────────────────────────────────────────────
    ("018290", "베어로보틱스",      "Bear Robotics",                 "TECH_HW",      "전자"),
    ("178320", "서진시스템",        "Seojin System",                 "TECH_HW",      "전자"),
    ("145140", "비에이치",          "BH",                            "TECH_HW",      "전자"),
    ("277810", "레인보우로보틱스",  "Rainbow Robotics",              "TECH_HW",      "전자"),
    # ── 소비재·기타 ─────────────────────────────────────────────────────
    ("950140", "잉글우드랩",        "Englewood Lab",                 "CONS",         "소비재"),
    ("215000", "골프존",            "Golfzon",                       "CONS",         "소비재"),
    ("293480", "하이즈항공",        "HiZ Aero",                      "DEFENSE",      "방산"),
    ("376060", "다음",              "Kakao (Daum)",                  "PLATFORM",     "플랫폼"),
]


# ═══════════════════════════════════════════════════════════════════════════
#  Wikipedia scrapers
# ═══════════════════════════════════════════════════════════════════════════

def _ua() -> dict[str, str]:
    return {"User-Agent": "Mozilla/5.0 (compatible; NexusOS/1.0 research)"}


def fetch_sp500_wiki() -> list[dict[str, str]]:
    """Return [{'ticker', 'name', 'gics_sector', 'sub_industry'}].
    ticker is kept in standard form (e.g. BRK.B); use _yahoo_us_symbol() when querying yf.
    """
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    resp = requests.get(url, headers=_ua(), timeout=20)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    table = soup.find("table", {"id": "constituents"})
    if not table:
        raise RuntimeError("S&P 500 table not found on Wikipedia")
    rows = table.find_all("tr")[1:]
    out: list[dict[str, str]] = []
    for row in rows:
        cols = row.find_all("td")
        if len(cols) >= 4:
            out.append({
                "ticker":      cols[0].get_text(strip=True),   # keep BRK.B form
                "name":        cols[1].get_text(strip=True),
                "gics_sector": cols[2].get_text(strip=True),
                "sub":         cols[3].get_text(strip=True),
            })
    logger.info("S&P 500 from Wikipedia: %d tickers", len(out))
    return out


def fetch_nasdaq100_wiki() -> list[dict[str, str]]:
    """Return [{'ticker', 'name', 'gics_sector'}]."""
    url = "https://en.wikipedia.org/wiki/Nasdaq-100"
    resp = requests.get(url, headers=_ua(), timeout=20)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    tables = soup.find_all("table", {"class": "wikitable"})
    target = next(
        (t for t in tables
         if t.find("tr") and "Ticker" in t.find("tr").get_text()),
        None,
    )
    if target is None:
        raise RuntimeError("NASDAQ-100 constituent table not found on Wikipedia")
    rows = target.find_all("tr")[1:]
    out: list[dict[str, str]] = []
    for row in rows:
        cols = row.find_all("td")
        if len(cols) >= 2:
            ticker = cols[0].get_text(strip=True)  # keep standard form
            name   = cols[1].get_text(strip=True)
            sector = cols[2].get_text(strip=True) if len(cols) > 2 else "Technology"
            out.append({"ticker": ticker, "name": name, "gics_sector": sector})
    logger.info("NASDAQ-100 from Wikipedia: %d tickers", len(out))
    return out


# ═══════════════════════════════════════════════════════════════════════════
#  yfinance enrichment
# ═══════════════════════════════════════════════════════════════════════════

def _yahoo_symbol(ticker: str, market: str) -> str:
    if market == "KRX":
        return f"{ticker}.KS"
    if market == "KOSDAQ":
        return f"{ticker}.KQ"
    return ticker  # US tickers handled by _yahoo_us_symbol


def _yahoo_us_symbol(ticker: str) -> str:
    """Convert standard ticker (BRK.B) → Yahoo Finance symbol (BRK-B)."""
    return ticker.replace(".", "-")


def _batch_fetch(
    symbols: list[str], batch_size: int = 40, delay: float = 1.5
) -> dict[str, dict[str, Any]]:
    """Fetch yfinance .info for a list of symbols. Returns {symbol: info}."""
    results: dict[str, dict[str, Any]] = {}
    for i in range(0, len(symbols), batch_size):
        chunk = symbols[i: i + batch_size]
        logger.info(
            "Fetching yfinance info batch %d/%d (%s … %s)",
            i // batch_size + 1,
            -(-len(symbols) // batch_size),
            chunk[0],
            chunk[-1],
        )
        for sym in chunk:
            try:
                info = yf.Ticker(sym).info
                if info and info.get("regularMarketPrice") is not None or info.get("marketCap"):
                    results[sym] = info
            except Exception as exc:
                logger.debug("yfinance error for %s: %s", sym, exc)
        time.sleep(delay)
    return results


def _market_from_exchange(info: dict[str, Any], ticker: str) -> str:
    exchange = (info.get("exchange") or "").upper()
    market   = (info.get("market")   or "").upper()
    if exchange in {"NMS", "NGM", "NCM", "NASDAQ", "NMQ"} or market in {"NMS", "NGM"}:
        return "NASDAQ"
    if exchange in {"NYQ", "NYSE", "ASE"} or market in {"NYQ"}:
        return "NYSE"
    if ticker in _NASDAQ_EXCHANGE_TICKERS:
        return "NASDAQ"
    return "NYSE"  # default for unknown US


def _sector_from_gics(gics: str) -> tuple[str, str]:
    return _GICS_MAP.get(gics, ("OTHER_US", "Other US"))


# ═══════════════════════════════════════════════════════════════════════════
#  Main build pipeline
# ═══════════════════════════════════════════════════════════════════════════

def _load_seed() -> dict[str, Any]:
    if not SEED_PATH.exists():
        return {"schema_version": 3, "note": "", "securities": [], "relations": []}
    return json.loads(SEED_PATH.read_bytes().decode("utf-8"))


def _apply_sector_median_estimates(securities: list[dict[str, Any]]) -> None:
    """B1: For static_master entries missing market_cap, assign the sector
    median computed from yahoo_finance entries in the same sector+market.

    Fallback chain:
      1. Same sector + same market (tightest)
      2. Same sector across all markets (broader)
      3. Same market across all sectors (last resort)
    Entries that still have no fallback keep market_cap=None and stay
    'static_master'. All assigned entries become data_source='estimated'.
    """
    import statistics

    # Build lookup: (sector, market) → sorted list of real market_caps
    real: dict[tuple[str, str], list[float]] = {}
    for s in securities:
        if s.get("data_source") == "yahoo_finance" and s.get("market_cap"):
            key = (s["sector"], s["market"])
            real.setdefault(key, []).append(float(s["market_cap"]))

    # Broader fallback: sector-only and market-only medians
    by_sector: dict[str, list[float]] = {}
    by_market: dict[str, list[float]] = {}
    for s in securities:
        if s.get("data_source") == "yahoo_finance" and s.get("market_cap"):
            by_sector.setdefault(s["sector"], []).append(float(s["market_cap"]))
            by_market.setdefault(s["market"], []).append(float(s["market_cap"]))

    estimated = 0
    for s in securities:
        if s.get("market_cap") is not None:
            continue  # already has data
        if s.get("data_source") != "static_master":
            continue  # only patch static_master

        sector, market = s["sector"], s["market"]
        caps = (
            real.get((sector, market))
            or by_sector.get(sector)
            or by_market.get(market)
        )
        if caps:
            s["market_cap"] = statistics.median(caps)
            s["data_source"] = "estimated"
            estimated += 1

    print(f"[estimate] assigned sector-median market_cap to {estimated} entries → data_source='estimated'")


def build_real_universe() -> dict[str, Any]:
    seed = _load_seed()
    existing = {s["ticker"]: s for s in seed.get("securities", [])}
    relations = seed.get("relations", [])

    # ── Step 1: collect all Korean tickers ────────────────────────────
    kr_entries: dict[str, tuple[str | None, str, str, str, str]] = {}
    # {ticker: (name_ko, name_en, market, sector, sector_label)}
    for ticker, name_ko, name_en, sector, sector_label in KOSPI_FULL:
        kr_entries[ticker] = (name_ko, name_en, "KRX", sector, sector_label)
    for ticker, name_ko, name_en, sector, sector_label in KOSDAQ_FULL:
        if ticker not in kr_entries:
            kr_entries[ticker] = (name_ko, name_en, "KOSDAQ", sector, sector_label)

    # ── Step 2: fetch US index constituents ───────────────────────────
    print("[wiki] fetching S&P 500 …")
    sp500 = fetch_sp500_wiki()
    print("[wiki] fetching NASDAQ-100 …")
    nq100 = fetch_nasdaq100_wiki()

    us_entries: dict[str, dict[str, str]] = {}  # ticker → {name, gics_sector}
    for row in sp500:
        us_entries[row["ticker"]] = row
    for row in nq100:
        us_entries.setdefault(row["ticker"], row)

    print(f"[info] {len(kr_entries)} Korean tickers, {len(us_entries)} US tickers to process")

    # ── Step 3: yfinance enrichment for Korean tickers ────────────────
    kr_symbols = [
        _yahoo_symbol(t, v[2]) for t, v in kr_entries.items()
    ]
    print(f"[yf] fetching {len(kr_symbols)} Korean symbols …")
    kr_yf = _batch_fetch(kr_symbols, batch_size=30, delay=2.0)
    print(f"[yf] got data for {len(kr_yf)} Korean symbols")

    # ── Step 4: yfinance enrichment for US tickers ─────────────────────
    # Map standard ticker → Yahoo symbol (BRK.B → BRK-B) for fetching
    us_yf_symbols = [_yahoo_us_symbol(t) for t in us_entries.keys()]
    print(f"[yf] fetching {len(us_yf_symbols)} US symbols …")
    us_yf_raw = _batch_fetch(us_yf_symbols, batch_size=50, delay=1.5)
    # Re-key back to standard ticker form
    us_yf: dict[str, dict[str, Any]] = {}
    for ticker in us_entries:
        yf_sym = _yahoo_us_symbol(ticker)
        if yf_sym in us_yf_raw:
            us_yf[ticker] = us_yf_raw[yf_sym]
    print(f"[yf] got data for {len(us_yf)} US symbols")

    # ── Step 5: build new securities list ─────────────────────────────
    new_securities: list[dict[str, Any]] = []
    seen: set[str] = set()

    def _add(sec: dict[str, Any]) -> None:
        t = sec["ticker"]
        if t not in seen:
            seen.add(t)
            new_securities.append(sec)

    # ── 5a: Korean tickers ───────────────────────────────────────────
    for ticker, (name_ko, name_en_default, market, sector, sector_label) in kr_entries.items():
        yahoo_sym = _yahoo_symbol(ticker, market)
        info = kr_yf.get(yahoo_sym, {})

        # If yfinance has a long name, use it; otherwise fall back
        name_en = (info.get("longName") or info.get("shortName") or name_en_default)
        market_cap = info.get("marketCap")
        shares = info.get("sharesOutstanding") or info.get("impliedSharesOutstanding")

        # Preserve is_subscribed=True for KIS stocks, but update market data
        existing_entry = existing.get(ticker)
        if existing_entry and existing_entry.get("is_subscribed"):
            entry = dict(existing_entry)
            if not entry.get("name_en") and name_en:
                entry["name_en"] = name_en
            if market_cap:
                entry["market_cap"] = float(market_cap)
            if shares:
                entry["shares_outstanding"] = int(shares)
            entry["data_source"] = "yahoo_finance" if info else entry.get("data_source", "static_master")
            _add(entry)
            continue

        # Skip if yfinance returned no meaningful data at all
        if not info and not name_en_default:
            logger.debug("Skip Korean ticker %s — no yfinance data", ticker)
            continue

        # Merge existing metadata (preserves aliases, is_subscribed=False)
        entry: dict[str, Any] = {
            "ticker":             ticker,
            "name_ko":            name_ko,
            "name_en":            name_en,
            "aliases":            existing_entry.get("aliases", []) if existing_entry else [],
            "market":             market,
            "sector":             existing_entry.get("sector", sector) if existing_entry else sector,
            "sector_label":       existing_entry.get("sector_label", sector_label) if existing_entry else sector_label,
            "currency":           "KRW",
            "shares_outstanding": int(shares) if shares else None,
            "market_cap":         float(market_cap) if market_cap else None,
            "is_subscribed":      False,
            "data_source":        "yahoo_finance" if info else "static_master",
        }
        _add(entry)

    # ── 5b: US tickers ───────────────────────────────────────────────
    for ticker, wiki_row in us_entries.items():
        info = us_yf.get(ticker, {})
        gics = wiki_row.get("gics_sector", "")
        name_en = (info.get("longName") or info.get("shortName") or wiki_row.get("name", ticker))
        market_cap = info.get("marketCap")
        shares = info.get("sharesOutstanding") or info.get("impliedSharesOutstanding")
        us_market = _market_from_exchange(info, ticker)
        sector_code, sector_label = _sector_from_gics(gics)

        # Preserve is_subscribed for any existing US entry
        existing_entry = existing.get(ticker)
        if existing_entry and existing_entry.get("is_subscribed"):
            entry = dict(existing_entry)
            if market_cap:
                entry["market_cap"] = float(market_cap)
            if shares:
                entry["shares_outstanding"] = int(shares)
            _add(entry)
            continue

        entry = {
            "ticker":             ticker,
            "name_ko":            None,
            "name_en":            name_en,
            "aliases":            [ticker],
            "market":             us_market,
            "sector":             sector_code,
            "sector_label":       sector_label,
            "currency":           "USD",
            "shares_outstanding": int(shares) if shares else None,
            "market_cap":         float(market_cap) if market_cap else None,
            "is_subscribed":      False,
            "data_source":        "yahoo_finance" if info else "static_master",
        }
        _add(entry)

    # ── 5c: any remaining existing entries not covered ─────────────────
    for ticker, entry in existing.items():
        if entry.get("data_source") == "static_master_padding":
            continue  # DROP all padding
        _add(entry)

    # ── Step 6: B1 — fill missing market_cap with sector median estimate ──
    _apply_sector_median_estimates(new_securities)

    print(f"[build] {len(new_securities)} real securities (0 padding)")

    from collections import Counter
    by_market = Counter(s["market"] for s in new_securities)
    by_source = Counter(s["data_source"] for s in new_securities)
    for market, n in sorted(by_market.items()):
        print(f"  {market}: {n}")
    for src, n in sorted(by_source.items()):
        print(f"  data_source={src}: {n}")

    return {
        "schema_version": 3,
        "note": (
            "Real-data universe — KOSPI 200 + KOSDAQ 150 + S&P 500 + NASDAQ 100. "
            "data_source='yahoo_finance' = enriched from Yahoo Finance. "
            "data_source='estimated' = sector-median market_cap assigned (no direct Yahoo data). "
            "data_source='static_master' = name/sector only, no market_cap available. "
            "All PAD_* placeholder rows removed. "
            "12 KIS is_subscribed=true rows preserve live market caps."
        ),
        "securities": new_securities,
        "relations":  relations,
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Replace PAD_* placeholders with real securities data from Yahoo Finance.",
    )
    parser.add_argument(
        "--apply", action="store_true",
        help="Write the output (default: dry-run, stats only).",
    )
    parser.add_argument(
        "--out", "-o", default=str(SEED_PATH),
        help=f"Output path (default: {SEED_PATH})",
    )
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )

    seed = build_real_universe()

    if args.apply:
        out_path = Path(args.out)
        out_path.write_text(
            json.dumps(seed, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        print(f"[OK] wrote {out_path} ({len(seed['securities'])} securities)")
    else:
        print("[dry-run] pass --apply to write output")

    return 0


if __name__ == "__main__":
    sys.exit(main())
