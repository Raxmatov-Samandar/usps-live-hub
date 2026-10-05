# USPS Live Hub

Bitta dastur uchta ishni bajaradi: usps.uz'dan yuklarni oladi, Telegram kanalga post qiladi va o'z board sahifangizda real vaqtda ko'rsatadi. Eski `main.py` ga endi ehtiyoj yo'q, bu uning o'rnini to'liq bosadi.

## Ishga tushirish (kompyuterda)
```powershell
cd "D:\Dasturlar\USPS Live Hub"
pip install -r requirements.txt
python app.py
```
Brauzerda oching: **http://localhost:8000/board**

`.env` faylini eski papkadan ko'chirib olsangiz bo'ladi, faqat ichiga `BOARD_URL=` va `PORT=8000` qatorlarini qo'shing.

## Board imkoniyatlari
- Yangi yuk kanalga tushishi bilan sahifada ham darhol paydo bo'ladi (yangilash shart emas), istasangiz ovozli signal bilan.
- Har bir yukda 30 daqiqalik qayta sanoq va chiziq: yashil (15+ daqiqa), sariq (5–15), qizil (oxirgi 5 daqiqa).
- Filtrlar: status, Team/Solo, shtat (bir nechtasini tanlash mumkin), qidiruv (load raqami, shahar, ZIP), saralash. Tanlangan filtrlar brauzerda eslab qolinadi.
- Yuk bosilganda batafsil oyna ochiladi: to'liq manzillar, Google Maps marshruti, Telegram'dagi posti, load raqami va linkini nusxalash.
- Kanaldagi Board tugmasi `.../board?load=125978035` ko'rinishida bo'lib, aynan o'sha yukni ochadi.
- Telefonda ham qulay ishlaydi, kunduzgi va tungi mavzu avtomatik almashadi.
- Yuklar `loads.db` bazasida 30 kun saqlanadi.

## Serverga qo'yish
Server va domen olingach, ikkita narsa kerak bo'ladi:
1. systemd orqali `python3 app.py` ni doimiy ishga tushirish.
2. Nginx + bepul SSL (Let's Encrypt) orqali `https://domen.uz` ni 8000-portga ulash, shundan keyin `.env` ga `BOARD_URL=https://domen.uz/board` yoziladi.

Bu qadamlarni server olinganda birga qilamiz.
