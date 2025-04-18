import os
import re
import random
import time
import logging
import pandas as pd
from datetime import datetime
from urllib.parse import urlparse
from sqlalchemy import create_engine, text
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import WebDriverException, TimeoutException
from webdriver_manager.chrome import ChromeDriverManager
from telegram import Update, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import Application, MessageHandler, filters, CommandHandler, ContextTypes
from dotenv import load_dotenv

load_dotenv()
TOKEN = os.getenv("TELEGRAM_TOKEN")

engine = create_engine("sqlite:///sites.db")

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
]


def get_chrome_driver():
    chrome_options = Options()

    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")

    chrome_options.add_argument(f"user-agent={random.choice(USER_AGENTS)}")

    chrome_options.add_argument("--disable-webgl")
    chrome_options.add_argument("--disable-3d-apis")
    chrome_options.add_argument("--log-level=3")

    service = Service(
        executable_path=r"C:\Users\Admin\Downloads\chromedriver-win64 (1)\chromedriver-win64\chromedriver.exe")

    return webdriver.Chrome(
        service=service,
        options=chrome_options,
        service_log_path=None
    )


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    upload_button = KeyboardButton(
        "Загрузить файл 📂",
        request_document=True
    )
    keyboard = [[upload_button]]
    await update.message.reply_text(
        "Отправьте Excel-файл:",
        reply_markup=ReplyKeyboardMarkup(
            [[upload_button]],
            resize_keyboard=True,
            one_time_keyboard=True
        )
    )


async def handle_file(update: Update, context: ContextTypes.DEFAULT_TYPE):
    filename = None

    document = update.message.document
    await update.message.reply_text(f"Файл {document.file_name} успешно загружен!")

    try:
        file = await update.message.effective_attachment.get_file()
        filename = f"temp_{datetime.now().timestamp()}.xlsx"
        await file.download_to_drive(filename)

        df = pd.read_excel(filename)
        required_columns = ["title", "url", "xpath"]

        if not all(col in df.columns for col in required_columns):
            raise ValueError("Файл должен содержать колонки: title, url, xpath")

        df["domain"] = df["url"].apply(lambda x: urlparse(x).netloc)
        df["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        df["price"] = None

        with engine.begin() as conn:
            df.to_sql("sites", conn, if_exists="replace", index=False)

        await parse_prices(update, df.to_dict("records"))
        await update.message.reply_text("✅ Данные успешно обработаны!")

    except Exception as e:
        logger.error(f"Ошибка: {str(e)}")
        await update.message.reply_text(f"❌ Ошибка: {str(e)}")
    finally:
        if filename and os.path.exists(filename):
            os.remove(filename)


async def parse_prices(update: Update, sites_data: list):
    driver = None
    try:
        driver = get_chrome_driver()
        success_count = 0

        for idx, site in enumerate(sites_data):
            try:
                time.sleep(random.uniform(1, 3))

                price = parse_single_price(driver, site)
                if price:
                    update_database(site["url"], price)
                    success_count += 1

                if idx % 10 == 9:
                    driver.quit()
                    driver = get_chrome_driver()

            except Exception as e:
                logger.error(f"Ошибка обработки {site['url']}: {str(e)}")

        await send_statistics(update)
        await update.message.reply_text(f"Успешно обработано: {success_count}/{len(sites_data)}")

    except WebDriverException as e:
        logger.error(f"Ошибка драйвера: {str(e)}")
        await update.message.reply_text("⚠️ Ошибка инициализации браузера")
    except Exception as e:
        logger.error(f"Критическая ошибка: {str(e)}")
        await update.message.reply_text("⚠️ Произошла непредвиденная ошибка")
    finally:
        if driver:
            driver.quit()


def parse_single_price(driver, site):
    try:
        driver.get(site["url"])

        if "Доступ ограничен" in driver.title:
            raise Exception("Обнаружена блокировка")

        element = WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((
                By.XPATH,
                '//*[contains(@class, "l9j")]//*[contains(text(), "₽") or contains(text(), "руб")]'
            ))
        )

        driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", element)
        time.sleep(0.5)

        price_text = element.text.strip()
        clean_price = re.sub(r'[^\d,]', '', price_text).replace(',', '.', 1)

        return float(clean_price) if clean_price else None

    except TimeoutException:
        logger.warning(f"Таймаут при загрузке: {site['url']}")
        return None
    except Exception as e:
        logger.error(f"Ошибка парсинга {site['url']}: {str(e)}")
        return None


def update_database(url, price):
    with engine.begin() as conn:
        conn.execute(text("""
            UPDATE sites 
            SET price = :price, 
                updated_at = :now 
            WHERE url = :url
        """), {
            "price": price,
            "url": url,
            "now": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })


async def send_statistics(update: Update):
    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT domain, 
                       ROUND(AVG(price), 2) as avg_price,
                       COUNT(price) as items
                FROM sites
                WHERE price IS NOT NULL
                GROUP BY domain
            """))
            stats = result.fetchall()

        if not stats:
            await update.message.reply_text("📊 Нет данных для отображения")
            return

        report = ["📊 Отчет по ценам:\n"]
        for domain, avg_price, count in stats:
            report.append(
                f"🌐 {domain}\n"
                f"   ▸ Средняя цена: {avg_price} руб.\n"
                f"   ▸ Товаров: {count}\n"
            )

        await update.message.reply_text("\n".join(report))
    except Exception as e:
        logger.error(f"Ошибка формирования отчета: {str(e)}")


def main():
    application = Application.builder().token(TOKEN).build()
    application.add_handler(CommandHandler("start", start))
    application.add_handler(MessageHandler(filters.Document.ALL, handle_file))
    application.run_polling()


if __name__ == "__main__":
    main()