"""
hh.uz (butun O'zbekiston bo'yicha) saytidan "data analytics"ga oid yangi
vakansiyalarni topib, Telegram botga yuboradi.

MUHIM: hh.ru/hh.uz 2026-yil aprelidan boshlab api.hh.ru JSON API'sini
autentifikatsiyasiz so'rovlar uchun yopib qo'ygan (403/400 xato beradi).
Shu sababli bu skript hali ham ochiq bo'lgan qidiruv RSS-lentasidan
foydalanadi: https://tashkent.hh.uz/search/vacancy/rss

Ishlash printsipi:
- Har safar ishga tushganda RSS-lentadan Toshkentdagi data analytics
  vakansiyalarini oladi.
- seen_ids.json faylida avval yuborilgan vakansiyalar linkini saqlaydi,
  shu bois har vakansiya faqat BIR MARTA yuboriladi.
- GitHub Actions orqali muntazam (masalan har 30 daqiqada) ishga tushiriladi.
"""

import html
import json
import os
import re
import time
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime

import requests

# ---------- SOZLAMALAR ----------
HH_AREA_ID = 97  # O'zbekiston (butun mamlakat, faqat Toshkent emas)
# hh.uz RSS'i "OR" mantiqini URL ichida to'g'ri qo'llamaydi (filtrsiz natija
# qaytarib yuboradi), shuning uchun har bir so'z alohida so'rov sifatida
# yuboriladi va natijalar keyin birlashtiriladi.
# Bu ro'yxat ataylab keng ildiz so'zlardan ("анализ", "tahlil" kabi) qochadi,
# chunki ular deyarli har qanday vakansiyada uchraydi (masalan "bozorni
# tahlil qilish", "moliyaviy anализ") va noaniq natija beradi. Buning o'rniga
# aynan data analytics kasbiga (va unga yaqin rollarga) tegishli aniq
# iboralar ishlatiladi.
SEARCH_KEYWORDS = [
    # --- Data Analyst / Analytics ---
    "data analyst",
    "data analytics",
    "аналитик данных",
    "аналитик по данным",
    "дата-аналитик",
    "дата аналитик",
    "data analytic",
    # --- Data Scientist / Engineer ---
    "data scientist",
    "data engineer",
    "дата-инженер",
    "инженер данных",
    "data engineering",
    "big data",
    "machine learning engineer",
    "ML engineer",
    # --- BI (Business Intelligence) ---
    "BI аналитик",
    "BI-аналитик",
    "business intelligence",
    "BI developer",
    "Power BI",
    "Tableau",
    "analytics engineer",
    # --- Yaqin/qo'shni rollar ---
    "product analyst",
    "продуктовый аналитик",
    "бизнес-аналитик",
    "business analyst",
    "quantitative analyst",
    "marketing analyst",
    "маркетинговый аналитик",
    "financial analyst",
    "финансовый аналитик",
    "web analytics",
    "веб-аналитик",
    # --- O'zbekcha ---
    "ma'lumotlar tahlilchisi",
    # --- Excel (ataylab yolg'iz "excel" emas, balki analitik lavozim
    # bilan birga birikma sifatida qo'shilgan — aks holda sarlavhasida
    # faqat "Excel" so'zi bo'lgan, data bilan aloqasi yo'q vakansiyalar
    # ham (masalan "Excel bo'yicha o'qituvchi") o'tib ketishi mumkin edi) ---
    "excel"
    # --- Intern / stajyor / junior (tajriba orttirish uchun, lekin
    # ataylab yolg'iz "intern"/"стажер" emas — sof holda qo'shilsa,
    # data'ga aloqasi yo'q har qanday soha stajyorligi ham (masalan
    # "Marketing Intern", "HR стажер") o'tib ketardi) ---
    "data analyst intern",
    "intern data analyst",
    "data analytics intern",
    "analytics intern",
    "стажер аналитик",
    "стажер дата-аналитик",
    "стажер data analyst",
    "junior data analyst",
    "junior analitik",
    "junior аналитик",
    "junior BI",
]

# Sarlavhada bu "ildiz" so'zlardan (butun so'z sifatida, boshqa harflar
# bilan qo'shilib ketmagan holda) biri uchrasa ham vakansiya qabul
# qilinadi: analitik/analyst/BI/data va ularning turli shakllari.
# \b (so'z chegarasi) tufayli "bilan", "database" kabi so'zlar ichidagi
# tasodifiy moslik hisobga olinmaydi.
# Bular prefiks sifatida qidiriladi (masalan "analitik" so'zi
# "analitikning", "analitikaga" kabi qo'shimchali shakllarni ham qamrab oladi)
ROOT_PREFIXES = [
    "analitik", "analitika",
    "analyst", "analytic", "analytics",
    "аналитик", "аналитика", "аналист",
    "data", "дата","excel",
]
# "bi" juda qisqa bo'lgani uchun faqat ALOHIDA SO'Z sifatida (masalan
# "BI aналитик", "Senior BI") qidiriladi — "biznes", "bilan" kabi
# so'zlar ichidagi tasodifiy moslikni chiqarib tashlash uchun.
ROOT_EXACT_WORDS = ["bi"]

ROOT_PATTERNS = [re.compile(rf"\b{re.escape(w)}\w*", re.IGNORECASE) for w in ROOT_PREFIXES]
ROOT_PATTERNS += [re.compile(rf"\b{re.escape(w)}\b", re.IGNORECASE) for w in ROOT_EXACT_WORDS]

# Sarlavhada quyidagi iboralardan biri uchrasa, vakansiya boshqa hech qanday
# kalit so'zga/ildizga mos kelsa ham RAD ETILADI (masalan "аналитик" ildiz
# so'ziga mos kelgani uchun "Системный аналитик" o'tib ketmasligi uchun).
EXCLUDE_KEYWORDS = [
    "системный аналитик",
    "систем аналитик",
    "тизим аналитиги",
    "tizim analitigi",
    "system analyst",
    "systems analyst",
]
EXCLUDE_PATTERNS = [re.compile(re.escape(w), re.IGNORECASE) for w in EXCLUDE_KEYWORDS]

RSS_URL = "https://hh.uz/search/vacancy/rss"
SEEN_IDS_FILE = "seen_ids.json"
LAST_RUN_FILE = "last_run.json"  # har run oxirida yoziladi — "run bo'ldi,
                                  # lekin yangi vakansiya topilmadi" holatini
                                  # ham iz sifatida saqlab qolish uchun
MAX_STORED_IDS = 2000  # fayl cheksiz o'sib ketmasligi uchun
MAX_RESULTS_PER_RUN = 20  # har ishga tushishda faqat eng oxirgi shuncha mos vakansiya ko'rib chiqiladi
MAX_AGE_DAYS = 3  # shundan eski e'lonlar "yangi" deb yuborilmaydi (kalit so'z
                   # ro'yxati kengaytirilganda eski vakansiyalar to'satdan mos
                   # kelib, "yangi" sifatida qayta yuborilib qolmasligi uchun)

MAX_WORKERS = 8  # bir vaqtda parallel yuboriladigan so'rovlar soni
MAX_RETRIES = 3  # 429 (Too Many Requests) xatosida qayta urinishlar soni
RETRY_BACKOFF_SECONDS = 3  # har qayta urinishda kutish (progressiv ravishda oshadi)

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")
TELEGRAM_API_URL = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )
}


def load_seen_ids():
    if os.path.exists(SEEN_IDS_FILE):
        with open(SEEN_IDS_FILE, "r", encoding="utf-8") as f:
            return set(json.load(f))
    return set()


def save_seen_ids(seen_ids):
    ids_list = list(seen_ids)[-MAX_STORED_IDS:]
    with open(SEEN_IDS_FILE, "w", encoding="utf-8") as f:
        json.dump(ids_list, f)


def save_last_run(status, matched_count, new_count, error=None):
    """Har run oxirida (natija qanday bo'lishidan qat'iy nazar) chaqiriladi.
    Bu fayl GitHub Actions workflow'da har safar commit qilinadi, shu bois
    repo tarixiga qarab 'run bo'lgan-bo'lmaganini' istalgan payt bilish
    mumkin bo'ladi — hatto yangi vakansiya topilmagan run'lar uchun ham."""
    data = {
        "last_run_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        "status": status,  # "ok" yoki "error"
        "matched_vacancies": matched_count,
        "new_sent": new_count,
    }
    if error:
        data["error"] = error
    try:
        with open(LAST_RUN_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except OSError as e:
        # last_run.json yozilmasa ham asosiy vazifa (vakansiya yuborish)
        # allaqachon bajarilgan, shu sababli bu xato skriptni to'xtatmasin
        print(f"[ogohlantirish] {LAST_RUN_FILE} yozilmadi: {e}")


def strip_html(text):
    """<p>...</p> kabi HTML teglarini olib tashlaydi va bo'sh joylarni tozalaydi."""
    text = re.sub(r"<[^>]+>", " ", text)
    text = html.unescape(text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def matched_keyword(title):
    """Sarlavhada SEARCH_KEYWORDS ro'yxatidagi to'liq iboralardan yoki
    ROOT_PREFIXES/ROOT_EXACT_WORDS ro'yxatidagi ildiz so'zlardan biri
    bor-yo'qligini tekshiradi. hh.uz RSS qidiruvi "fuzzy" ishlaydi
    (tavsifda yoki mos kelmaydigan bo'limda so'z uchrasa ham natija
    qaytaradi — masalan "Project Manager"), shuning uchun faqat
    SARLAVHADA aynan mos so'z bo'lgan vakansiyalar qabul qilinadi.
    Mos kelgan so'zni qaytaradi, aks holda None."""
    title_lower = title.lower()

    # 0) Avval istisnolar tekshiriladi — "Системный аналитик" kabi
    # sarlavhalar "аналитик" ildiziga mos kelsa ham chiqarib tashlanadi.
    for pattern in EXCLUDE_PATTERNS:
        if pattern.search(title_lower):
            return None

    # 1) Avval to'liq/aniq iboralar tekshiriladi (masalan "Power BI", "Tableau")
    for keyword in SEARCH_KEYWORDS:
        if keyword.lower() in title_lower:
            return keyword

    # 2) Keyin ildiz so'zlar tekshiriladi: "analitik", "analyst", "data",
    # "BI" va shu kabi barcha shakllar (masalan "Senior Data Analyst",
    # "Junior Analitik", "BI Developer")
    for pattern in ROOT_PATTERNS:
        match = pattern.search(title_lower)
        if match:
            return match.group(0)

    return None


def parse_pub_date(item):
    """RSS'dagi pubDate maydonini parse qiladi. hh.uz odatda RFC-2822
    formatini ishlatadi ("Mon, 17 Aug 2026 10:00:00 +0500"), lekin ehtiyot
    chorasi sifatida ISO-8601 formatini ham ("2026-08-17T10:00:00+05:00")
    sinab ko'ramiz.

    Agar pubDate bo'sh yoki parse qilib bo'lmasa (amalda hh.uz buni ko'pincha
    bo'sh qoldirar ekan), TAVSIF matnidagi "Создана: DD.MM.YYYY" formatidagi
    sanani zaxira manba sifatida ishlatamiz — bu aniq soatni bermaydi, lekin
    kun aniqligida ishonchli."""
    raw = item.findtext("pubDate", default="").strip()

    if raw:
        try:
            return parsedate_to_datetime(raw)
        except (TypeError, ValueError):
            pass
        try:
            dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except ValueError:
            pass

    # Zaxira: tavsifdagi "Создана: DD.MM.YYYY" sanasi (Toshkent vaqti, UTC+5)
    description_raw = item.findtext("description", default="")
    match = re.search(r"Создана:\s*(\d{2})\.(\d{2})\.(\d{4})", description_raw)
    if match:
        day, month, year = match.groups()
        try:
            tashkent_tz = timezone(timedelta(hours=5))
            dt = datetime(int(year), int(month), int(day), tzinfo=tashkent_tz)
            return dt.astimezone(timezone.utc)
        except ValueError:
            pass

    if raw:
        print(f"[debug] pubDate parse qilinmadi, xom qiymat: {raw!r}")
    else:
        print("[debug] pubDate bo'sh va tavsifda ham 'Создана:' sanasi topilmadi")
    return None


def fetch_vacancies_for_keyword(keyword):
    """Bitta kalit so'z uchun RSS'dan vakansiyalarni oladi.
    429 (Too Many Requests) xatosida MAX_RETRIES marta progressiv
    kutish bilan qayta urinadi. Boshqa xatolarda (tarmoq, parsing va h.k.)
    butun skriptni to'xtatmaslik uchun bo'sh ro'yxat qaytaradi va
    xatoni konsolga chiqaradi."""
    params = {
        "text": keyword,
        "area": HH_AREA_ID,
        "order_by": "publication_time",
    }

    resp = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(RSS_URL, params=params, headers=HEADERS, timeout=30)
            if resp.status_code == 429:
                wait = RETRY_BACKOFF_SECONDS * attempt
                print(f"[{keyword}] 429 (Too Many Requests) — {wait} sek kutib, qayta urinilmoqda ({attempt}/{MAX_RETRIES})")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            break
        except requests.RequestException as e:
            print(f"[{keyword}] So'rov xatosi: {e}")
            resp = None
            break
    else:
        # for-else: barcha urinishlar 429 bilan tugadi
        print(f"[{keyword}] {MAX_RETRIES} urinishdan keyin ham 429 xatosi — bu kalit so'z o'tkazib yuborildi")
        return []

    if resp is None:
        return []

    try:
        root = ET.fromstring(resp.content)
    except ET.ParseError as e:
        print(f"[{keyword}] RSS parsing xatosi: {e}")
        return []

    items = []
    for item in root.findall("./channel/item"):
        link = item.findtext("link", default="").strip()
        title = item.findtext("title", default="Noma'lum lavozim").strip()
        description = strip_html(item.findtext("description", default=""))
        pub_date = parse_pub_date(item)

        # Faqat sarlavhasi bizning kalit so'z/ildizlarimizdan biriga mos
        # keladigan vakansiyalarni qabul qilamiz (hh.uz'ning aloqasiz
        # natijalar chiqarishining oldini olish uchun, masalan "Project
        # Manager").
        if not matched_keyword(title):
            continue

        items.append({
            "id": link,  # link vakansiya uchun noyob identifikator sifatida ishlatiladi
            "title": title,
            "description": description,
            "link": link,
            "pub_date": pub_date,
        })
    return items


def fetch_vacancies():
    """Har bir kalit so'z uchun so'rovlarni PARALLEL (MAX_WORKERS ta bir
    vaqtda) yuboradi, natijalarni (link bo'yicha) takrorlanmasdan
    birlashtiradi, eng yangilaridan boshlab saralaydi va faqat oxirgi
    MAX_RESULTS_PER_RUN tasini qaytaradi.
    Bitta kalit so'z bo'yicha so'rov muvaffaqiyatsiz tugasa ham
    (fetch_vacancies_for_keyword ichida ushlanadi), qolgan kalit so'zlar
    natijalari yo'qolmaydi."""
    seen_links = set()
    all_items = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_keyword = {
            executor.submit(fetch_vacancies_for_keyword, keyword): keyword
            for keyword in SEARCH_KEYWORDS
        }
        for future in as_completed(future_to_keyword):
            keyword = future_to_keyword[future]
            try:
                items = future.result()
            except Exception as e:
                # Kutilmagan xato — shu kalit so'zni o'tkazib yuboramiz,
                # boshqalarga ta'sir qilmaydi.
                print(f"[{keyword}] Kutilmagan xato: {e}")
                items = []

            for item in items:
                if item["id"] and item["id"] not in seen_links:
                    seen_links.add(item["id"])
                    all_items.append(item)

    # pub_date bo'yicha eng yangisidan eskisiga saralaymiz (sana topilmasa eng oxiriga tushadi)
    epoch = datetime.min.replace(tzinfo=timezone.utc)
    all_items.sort(key=lambda v: v["pub_date"] or epoch, reverse=True)

    # MAX_AGE_DAYS'dan eski e'lonlarni chiqarib tashlaymiz — bular seen_ids'da
    # bo'lmasligi mumkin (masalan kalit so'z ro'yxati yangi kengaytirilgan
    # bo'lsa), lekin ular haqiqatda "yangi vakansiya" emas.
    cutoff = datetime.now(timezone.utc) - timedelta(days=MAX_AGE_DAYS)
    no_date_count = sum(1 for v in all_items if not v["pub_date"])
    fresh_items = [v for v in all_items if v["pub_date"] and v["pub_date"] >= cutoff]

    print(
        f"[debug] Kalit so'zlarga mos jami: {len(all_items)} | "
        f"sana topilmagan (chiqarib tashlandi): {no_date_count} | "
        f"oxirgi {MAX_AGE_DAYS} kun ichida: {len(fresh_items)}"
    )
    if all_items[:5]:
        print("[debug] Eng so'nggi 5 ta topilgan (filtrgacha):")
        for v in all_items[:5]:
            print(f"  - {v['pub_date']}: {v['title']}")

    return fresh_items[:MAX_RESULTS_PER_RUN]


# Tavsif (description) ichidan ajratib olinadigan maydonlar, aynan shu
# tartibda. hh.uz RSS tavsifi odatda "Вакансия компании: X Создана: DD.MM.YYYY
# Регион: Y Предполагаемый уровень месячного дохода: Z" ko'rinishida bitta
# qatorga yig'ilgan holda keladi (strip_html bo'sh joylarni birlashtiradi).
DESCRIPTION_FIELD_LABELS = [
    "Company vacancy",
    "Created",
    "Region",
    "Estimated monthly income level",
]

DEFAULT_FIELD_VALUE = "not specified"


def parse_description_fields(description):
    """Tavsif matnidan DESCRIPTION_FIELD_LABELS'dagi har bir maydonni ajratib
    oladi. Har bir maydon qiymati — o'zidan keyingi maydon labeli (yoki matn
    oxiri)gacha bo'lgan matn. Agar biror maydon topilmasa (hh.uz format
    o'zgartirsa yoki maydon umuman bo'lmasa), xabar baribir "sinmasligi" uchun
    DEFAULT_FIELD_VALUE qo'yiladi — shu tufayli chiqadigan xabar tuzilishi
    har doim bir xil (4 qator + sarlavha + havola) bo'lib qoladi."""
    fields = {}
    for i, label in enumerate(DESCRIPTION_FIELD_LABELS):
        next_labels = DESCRIPTION_FIELD_LABELS[i + 1:]
        if next_labels:
            lookahead = "|".join(re.escape(l) for l in next_labels)
            pattern = rf"{re.escape(label)}:\s*(.*?)(?=(?:{lookahead}):|$)"
        else:
            pattern = rf"{re.escape(label)}:\s*(.*)$"
        match = re.search(pattern, description)
        value = match.group(1).strip() if match else ""
        fields[label] = value if value else DEFAULT_FIELD_VALUE
    return fields


def format_message(vacancy):
    """Har bir vakansiya uchun XABAR TUZILISHI DOIM BIR XIL bo'lishini
    kafolatlaydi: sarlavha, so'ngra 4 ta belgilangan maydon (har biri o'z
    qatorida, o'z labeli bilan), so'ngra havola — hh.uz'ning xom tavsif
    matni qanday kelishidan qat'i nazar."""
    fields = parse_description_fields(vacancy["description"])
    lines = [f"📊 <b>{vacancy['title']}</b>"]
    for label in DESCRIPTION_FIELD_LABELS:
        lines.append(f"{label}: {fields[label]}")
    lines.append(f"🔗 {vacancy['link']}")
    return "\n".join(lines)


def send_to_telegram(text):
    """True/False qaytaradi — muvaffaqiyatli yuborildimi yoki yo'q.
    Tarmoq/Telegram xatosida butun skriptni to'xtatmaydi, faqat shu
    vakansiyani "yuborilmadi" deb belgilaydi (seen_ids'ga qo'shilmaydi,
    keyingi ishga tushishda qayta urinilishi mumkin)."""
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": False,
    }
    try:
        resp = requests.post(TELEGRAM_API_URL, data=payload, timeout=30)
        if not resp.ok:
            print(f"Telegramga yuborishda xatolik: {resp.status_code} {resp.text}")
            return False
        return True
    except requests.RequestException as e:
        print(f"Telegramga yuborishda tarmoq xatosi: {e}")
        return False


def main():
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        raise SystemExit(
            "TELEGRAM_BOT_TOKEN va TELEGRAM_CHAT_ID environment variable "
            "sifatida berilishi kerak."
        )

    seen_ids = load_seen_ids()
    vacancies = []
    new_count = 0
    status = "ok"
    error_msg = None

    try:
        vacancies = fetch_vacancies()
        for vacancy in vacancies:
            vac_id = vacancy["id"]
            if not vac_id or vac_id in seen_ids:
                continue

            message = format_message(vacancy)
            sent_ok = send_to_telegram(message)
            if sent_ok:
                seen_ids.add(vac_id)
                new_count += 1
                # Har muvaffaqiyatli yuborishdan keyin DARHOL saqlaymiz —
                # shunda agar keyingi vakansiyani yuborishda xato bo'lsa
                # yoki skript to'xtab qolsa, hozirgacha yuborilganlar
                # qayta yuborilib qolmaydi.
                save_seen_ids(seen_ids)
            time.sleep(1)  # Telegram rate limit uchun kichik pauza
    except Exception as e:
        # Kutilmagan xato bo'lsa ham, run "iz qoldirishi" uchun
        # last_run.json baribir yoziladi (pastdagi finally'da).
        status = "error"
        error_msg = str(e)
        print(f"[xato] Run davomida kutilmagan xato: {e}")
        raise
    finally:
        # Har ehtimolga qarshi yakunida yana bir bor saqlaymiz.
        save_seen_ids(seen_ids)
        # Natija qanday bo'lishidan qat'iy nazar (topilma bor/yo'q, xato
        # bor/yo'q) — bu run haqiqatan ham ishga tushganini tasdiqlovchi iz.
        save_last_run(status, matched_count=len(vacancies), new_count=new_count, error=error_msg)

    print(f"Tekshiruv tugadi. Yangi yuborilgan vakansiyalar soni: {new_count}")


if __name__ == "__main__":
    main()
