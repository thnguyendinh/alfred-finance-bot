import logging
import os
import json
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, filters, CallbackQueryHandler, ContextTypes
import pymongo
from pymongo import MongoClient
from datetime import datetime, timedelta
from dateutil import parser
import spacy
from transformers import pipeline
from fuzzywuzzy import fuzz
import pandas as pd
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.cron import CronTrigger
from lunardate import LunarDate
import yfinance as yf
import requests
from bs4 import BeautifulSoup
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from aiohttp import web

# Env vars from Render
TELEGRAM_TOKEN = os.environ['TELEGRAM_TOKEN']
MONGO_URI = os.environ['MONGO_URI']
GOOGLE_CREDENTIALS_JSON = os.environ.get('GOOGLE_CREDENTIALS_JSON', '{}')

# Kết nối MongoDB
client = MongoClient(MONGO_URI)
db = client['alfred_finance']
nlp = spacy.load("vi_core_news_sm")
classifier = pipeline("zero-shot-classification", model="distilbert-base-uncased")
generator = pipeline("text-generation", model="distilgpt2")
#nlp = spacy.load("vi_core_news_sm")
#classifier = pipeline("zero-shot-classification", model="facebook/bart-large-mnli")
#generator = pipeline("text-generation", model="gpt2")
scheduler = AsyncIOScheduler()

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.message.from_user.id
    user_data = db['users'].find_one({'user_id': user_id})
    if not user_data:
        db['users'].insert_one({
            'user_id': user_id,
            'budget': {'monthly': 0, 'needs': 0.5, 'wants': 0.3, 'savings': 0.2},
            'expenses': [], 'debts': [], 'events': [], 'investments': [],
            'income': 0, 'assets': {'balance': 0}, 'reminders_enabled': True
        })
    await update.message.reply_text('Thưa ngài, Alfred Finance sẵn sàng phục vụ tài chính và sự kiện của ngài!')
    scheduler.start()

async def set_budget(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        monthly_income = float(context.args[0])
        user_id = update.message.from_user.id
        db['users'].update_one({'user_id': user_id}, {'$set': {'income': monthly_income}})
        await update.message.reply_text(f'Ngân sách: {monthly_income} VND. Phân bổ: 50% nhu cầu, 30% muốn, 20% tiết kiệm.')
    except:
        await update.message.reply_text('Thưa ngài, hãy nhập số tiền hợp lệ: /set_budget 20000000')

async def add_expense(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    keyboard = [
        [InlineKeyboardButton("Food", callback_data='Food'),
         InlineKeyboardButton("Transport", callback_data='Transport')],
        [InlineKeyboardButton("Entertainment", callback_data='Entertainment'),
         InlineKeyboardButton("Other", callback_data='Other')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text('Chọn category:', reply_markup=reply_markup)
    context.user_data['adding_expense'] = True
    context.user_data['expense_desc'] = ' '.join(context.args[:-1]) if context.args else 'No description'
    context.user_data['expense_amount'] = float(context.args[-1]) if context.args else 0

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    if 'adding_expense' in context.user_data:
        category = query.data
        desc = context.user_data.get('expense_desc', 'No desc')
        amount = context.user_data.get('expense_amount', 0)
        user_id = query.from_user.id
        expense = {'date': datetime.now(), 'desc': desc, 'amount': amount, 'category': category}
        db['users'].update_one({'user_id': user_id}, {'$push': {'expenses': expense}})
        user_data = db['users'].find_one({'user_id': user_id})
        total_expenses = sum(e['amount'] for e in user_data['expenses'] if e['category'] in ['Entertainment', 'Other'])
        wants_budget = user_data['income'] * 0.3
        if total_expenses > wants_budget:
            await query.edit_message_text(f'Chi tiêu: {desc} - {amount} VND ({category}). Vượt mức! Có lẽ ngài đang xây Batmobile?')
        else:
            await query.edit_message_text(f'Chi tiêu: {desc} - {amount} VND ({category}). Ghi nhận.')
        del context.user_data['adding_expense']

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.message.from_user.id
    text = update.message.text.lower()
    doc = nlp(text)
    
    intents = ['expense', 'debt', 'event', 'question']
    event_types = ['đám cưới', 'sinh nhật', 'họp lớp', 'du lịch', 'mua sắm lớn', 'other']
    intent_result = classifier(text, candidate_labels=intents)
    intent = intent_result['labels'][0] if intent_result['scores'][0] > 0.6 else 'unknown'
    
    amount = 0
    date = None
    desc = text
    is_lunar = 'am' in text or 'lunar' in text
    for ent in doc.ents:
        if ent.label_ == 'MONEY': amount = float(ent.text.replace('k', '000').replace(',', ''))
        if ent.label_ == 'DATE': date = parser.parse(ent.text, dayfirst=True)
    
    if not date:
        try: date = parser.parse(text.split()[-1], dayfirst=True)
        except: pass
    if date and date.year == 1900: date = date.replace(year=datetime.now().year)
    
    user_data = db['users'].find_one({'user_id': user_id})
    
    if intent == 'expense':
        category = "Food" if fuzz.ratio(text, "an uong") > 70 else "Other"
        expense = {'date': datetime.now(), 'desc': desc, 'amount': amount, 'category': category}
        db['users'].update_one({'user_id': user_id}, {'$push': {'expenses': expense}})
        df = pd.DataFrame(user_data['expenses'])
        total_wants = df[df['category'].isin(['Entertainment', 'Other'])]['amount'].sum()
        wants_budget = user_data['income'] * 0.3
        if total_wants > wants_budget:
            advice = generator("Gợi ý tiết kiệm khi vượt chi tiêu: ", max_length=50)[0]['generated_text']
            await update.message.reply_text(f'Chi tiêu: {amount} VND. Vượt mức! {advice.strip()}.')
        else:
            await update.message.reply_text(f'Chi tiêu: {amount} VND ({category}).')
    
    elif intent == 'debt':
        debt_entry = {'date': datetime.now(), 'desc': desc, 'amount': amount, 'due_date': date}
        db['users'].update_one({'user_id': user_id}, {'$push': {'debts': debt_entry}})
        advice = generator("Lời khuyên xử lý nợ: ", max_length=50)[0]['generated_text']
        await update.message.reply_text(f'Nợ: {amount} VND. {advice.strip()} Để lợi ích ngài!')
    
    elif intent == 'event':
        event_result = classifier(text, candidate_labels=event_types)
        event_type = event_result['labels'][0] if event_result['scores'][0] > 0.6 else 'other'
        if event_type == 'other' and fuzz.ratio(text, "mua xe") > 70: event_type = 'mua sắm lớn'
        
        if not date:
            await update.message.reply_text('Thưa ngài, vui lòng cung cấp ngày (ví dụ: 16/2).')
            return
        
        if is_lunar:
            lunar = LunarDate(date.year, date.month, date.day)
            solar_date = lunar.toSolarDate()
            date = datetime(solar_date.year, solar_date.month, solar_date.day)
            await update.message.reply_text(f'Lịch âm {date.day}/{date.month} → dương: {date.strftime("%d/%m/%Y")}')
        
        cost_estimate = {'đám cưới': 5000000, 'sinh nhật': 1000000, 'họp lớp': 2000000, 'du lịch': 10000000, 'mua sắm lớn': 50000000}.get(event_type, 1000000)
        gift_ideas = generator(f"Gợi ý quà cho {event_type}: ", max_length=50)[0]['generated_text']
        event = {'date': date, 'desc': desc, 'type': event_type, 'is_lunar': is_lunar, 'reminders': {'cost_estimate': cost_estimate, 'gift_ideas': gift_ideas}}
        db['users'].update_one({'user_id': user_id}, {'$push': {'events': event}})
        
        async def send_reminder():
            await context.bot.send_message(chat_id=update.message.chat_id, text=f'Thưa ngài, hôm nay {event_type}: Dự trù {cost_estimate} VND. Quà: {gift_ideas}.')
        scheduler.add_job(send_reminder, DateTrigger(run_date=date))
        
        await update.message.reply_text(f'Sự kiện {event_type} ngày {date.strftime("%d/%m/%Y")}. Gợi ý: {gift_ideas}. Alfred sẽ nhắc!')
    
    elif intent == 'question':
        answer = generator(f"Trả lời tài chính: {text}", max_length=100)[0]['generated_text']
        await update.message.reply_text(f'Thưa ngài, lời khuyên: {answer.strip()}')
    
    else:
        await update.message.reply_text('Thưa ngài, Alfred chưa hiểu. Hãy thử lại!')
        if 'goi y mo hinh' in text or 'suggest model' in text:
            await suggest_model(update, context)

async def report(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.message.from_user.id
    user_data = db['users'].find_one({'user_id': user_id})
    df_exp = pd.DataFrame(user_data['expenses'])
    summary = df_exp.groupby('category')['amount'].sum().to_string()
    forecast = df_exp['amount'].mean() * 30 if not df_exp.empty else 0
    events = [f"{e['type']} ({e['date'].strftime('%d/%m/%Y')})" for e in user_data['events'] if e['date'] > datetime.now()]
    await update.message.reply_text(f'Báo cáo: {summary}\nDự báo tháng: {forecast} VND\nSự kiện sắp tới: {", ".join(events)}.')

async def suggest_model(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.message.from_user.id
    user_data = db['users'].find_one({'user_id': user_id})
    if not user_data or user_data['income'] == 0:
        await update.message.reply_text('Thưa ngài, hãy /set_budget trước!')
        return
    
    df_exp = pd.DataFrame(user_data.get('expenses', []))
    df_debts = pd.DataFrame(user_data.get('debts', []))
    df_events = pd.DataFrame(user_data.get('events', []))
    
    total_exp = df_exp['amount'].sum()
    total_debt = df_debts['amount'].sum()
    income = user_data['income']
    num_events = len(df_events)
    
    status_text = f"Thu nhập: {income}, chi tiêu: {total_exp}, nợ: {total_debt}, sự kiện: {num_events}"
    status_labels = ['high debt', 'high spending', 'savings goal', 'stable', 'event focused']
    status_result = classifier(status_text, candidate_labels=status_labels)
    user_status = status_result['labels'][0]
    
    models = [
        "50/30/20 Rule: 50% nhu cầu, 30% muốn, 20% tiết kiệm.",
        "Zero-Based Budgeting: Phân bổ mọi đồng tiền cụ thể.",
        "Debt Snowball: Trả nợ nhỏ trước để tạo động lực.",
        "Debt Avalanche: Trả nợ lãi cao trước để tiết kiệm lãi.",
        "Pay Yourself First: Tiết kiệm trước khi chi tiêu.",
        "Envelope System: Phân bổ tiền vào 'phong bì' category."
    ]
    
    prompt = f"Gợi ý mô hình quản lý tài chính phù hợp nhất cho tình trạng: {user_status}, dư nợ {total_debt}, mục đích {num_events} sự kiện. Từ danh sách: {', '.join(models)}. Lý do và cách áp dụng:"
    suggestion = generator(prompt, max_length=150)[0]['generated_text']
    
    await update.message.reply_text(f'Thưa ngài, dựa trên tình trạng hiện tại (nợ: {total_debt}, chi: {total_exp}/{income}), Alfred gợi ý:\n{suggestion}\nCác mô hình khác: {", ".join(models)}')

async def toggle_reminders(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.message.from_user.id
    current = db['users'].find_one({'user_id': user_id}).get('reminders_enabled', True)
    db['users'].update_one({'user_id': user_id}, {'$set': {'reminders_enabled': not current}})
    await update.message.reply_text(f'Nhắc nhở đã {"bật" if not current else "tắt"}, thưa ngài.')

async def weekly_report(context: ContextTypes.DEFAULT_TYPE):
    for user in db['users'].find({'reminders_enabled': True}):
        user_data = user
        df_exp = pd.DataFrame(user_data['expenses'])
        summary = df_exp['amount'].sum()
        prompt = f"Tóm tắt tài chính tuần: chi tiêu {summary}, lời khuyên:"
        newsletter = generator(prompt, max_length=100)[0]['generated_text']
        await context.bot.send_message(chat_id=user['user_id'], text=f'Thưa ngài, newsletter tuần: {newsletter}')

async def check_investments(context: ContextTypes.DEFAULT_TYPE):
    for user in db['users'].find({'investments': {'$exists': True}}):
        for inv in user['investments']:
            ticker = {'gold': 'GC=F', 'btc': 'BTC-USD'}.get(inv['asset'].lower(), inv['asset'].upper() + '-USD')
            try:
                data = yf.download(ticker, period='1d')
                current = data['Close'].iloc[-1]
                change = (current - inv['buy_price']) / inv['buy_price'] * 100
                if abs(change) > 5:
                    await context.bot.send_message(chat_id=user['user_id'], text=f'Thưa ngài, {inv["asset"]} biến động {change:.2f}% – giá hiện {current:.2f}.')
                db['users'].update_one({'user_id': user['user_id'], 'investments.asset': inv['asset']}, {'$set': {'investments.$.current_value': current}})
            except:
                pass

scheduler.add_job(weekly_report, CronTrigger(day_of_week='mon', hour=8))
scheduler.add_job(check_investments, CronTrigger(hour=9))

async def get_price(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    asset = ' '.join(context.args).lower()
    if 'bds' in asset:
        url = 'https://batdongsan.com.vn/nha-dat-ban-ha-noi' if 'hanoi' in asset else 'https://batdongsan.com.vn/nha-dat-ban'
        try:
            response = requests.get(url)
            soup = BeautifulSoup(response.text, 'html.parser')
            prices = [p.text for p in soup.find_all('span', class_='product-price')]
            avg_price = prices[0] if prices else 'Không tìm thấy dữ liệu hiện tại.'
            await update.message.reply_text(f'Giá BDS {asset}: {avg_price}')
        except:
            await update.message.reply_text(f'Thưa ngài, không tìm thấy giá BDS {asset}. Hãy thử lại!')
    else:
        ticker = {'gold': 'GC=F', 'btc': 'BTC-USD', 'vn-index': '^VNI'}.get(asset, asset.upper() + '-USD')
        try:
            data = yf.download(ticker, period='1d')
            price = data['Close'].iloc[-1]
            await update.message.reply_text(f'Giá {asset.upper()}: {price:.2f} (USD hoặc VND tương đương).')
        except:
            await update.message.reply_text(f'Thưa ngài, không tìm thấy giá {asset}. Hãy thử lại!')

async def invest_advice(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    asset = ' '.join(context.args).lower()
    ticker = {'gold': 'GC=F', 'btc': 'BTC-USD'}.get(asset, asset.upper() + '-USD')
    try:
        data = yf.download(ticker, period='1mo')
        change = (data['Close'].iloc[-1] - data['Close'].iloc[0]) / data['Close'].iloc[0] * 100
        prompt = f"Đánh giá {asset}: biến động {change:.2f}%, lời khuyên đầu tư:"
        advice = generator(prompt, max_length=100)[0]['generated_text']
        await update.message.reply_text(f'Thưa ngài, {advice}')
    except:
        await update.message.reply_text(f'Thưa ngài, không tìm thấy dữ liệu cho {asset}. Hãy thử lại!')

async def add_investment(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if len(context.args) < 3:
        await update.message.reply_text('Thưa ngài, dùng /add_investment asset amount buy_price')
        return
    asset, amount, buy_price = context.args
    user_id = update.message.from_user.id
    investment = {'asset': asset, 'amount': float(amount), 'buy_price': float(buy_price), 'current_value': 0}
    db['users'].update_one({'user_id': user_id}, {'$push': {'investments': investment}})
    await update.message.reply_text(f'Đầu tư {asset} thêm, thưa ngài. Alfred sẽ theo dõi.')

async def export_to_sheets(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args:
        await update.message.reply_text('Thưa ngài, dùng /export_to_sheets sheet_id')
        return
    sheet_id = context.args[0]
    user_id = update.message.from_user.id
    user_data = db['users'].find_one({'user_id': user_id})
    try:
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_dict(json.loads(GOOGLE_CREDENTIALS_JSON), scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_key(sheet_id).sheet1
        expenses = [[e['date'].strftime('%Y-%m-%d'), e['desc'], e['amount'], e['category']] for e in user_data['expenses']]
        sheet.update('A1', [['Date', 'Desc', 'Amount', 'Category']] + expenses)
        await update.message.reply_text('Dữ liệu export sang Sheets, thưa ngài.')
    except:
        await update.message.reply_text('Thưa ngài, lỗi export. Kiểm tra sheet_id hoặc credentials!')

async def webhook_handler(request):
    update = Update.de_json(await request.json(), application.bot)
    await application.process_update(update)
    return web.Response(text="OK")

async def main():
    global application
    application = Application.builder().token(TELEGRAM_TOKEN).build()
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("set_budget", set_budget))
    application.add_handler(CommandHandler("add_expense", add_expense))
    application.add_handler(CommandHandler("report", report))
    application.add_handler(CommandHandler("suggest_model", suggest_model))
    application.add_handler(CommandHandler("toggle_reminders", toggle_reminders))
    application.add_handler(CommandHandler("get_price", get_price))
    application.add_handler(CommandHandler("invest_advice", invest_advice))
    application.add_handler(CommandHandler("add_investment", add_investment))
    application.add_handler(CommandHandler("export_to_sheets", export_to_sheets))
    application.add_handler(CallbackQueryHandler(button_handler))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    
    webhook_url = f"https://{os.environ['RENDER_EXTERNAL_HOSTNAME']}/webhook"
    await application.bot.set_webhook(url=webhook_url)
    
    app = web.Application()
    app.router.add_post('/webhook', webhook_handler)
    return app

if __name__ == '__main__':
    web.run_app(main(), host='0.0.0.0', port=int(os.environ.get('PORT', 8443)))
