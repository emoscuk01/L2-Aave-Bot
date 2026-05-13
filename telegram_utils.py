"""
telegram_utils.py — Fire-and-Forget Telegram Bildirimleri v2
-------------------------------------------------------------
Tüm Telegram mesajları asyncio.create_task() ile arka plana atılır.
Ana döngü tek milisaniye beklemez.

Mesaj formatları talep edilen şablona göre HTML ile yazılmıştır.
"""

import asyncio
import logging
import os
from datetime import datetime
from typing import Optional

import aiohttp

logger = logging.getLogger(__name__)

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID", "")


def _now() -> str:
    return datetime.now().strftime("%d.%m.%Y %H:%M:%S")


class TelegramNotifier:
    """
    Fire-and-forget Telegram bildirici.
    send() çağrısı event loop'a task ekler ve HEMEN döner — await yok.
    Hata olursa sessizce loglanır, ana döngüyü asla etkile mez.
    """

    def __init__(self, token: str = BOT_TOKEN, chat_id: str = CHAT_ID):
        self.token   = token
        self.chat_id = chat_id
        self.url     = f"https://api.telegram.org/bot{token}/sendMessage"
        self.enabled = bool(token and chat_id)

        if not self.enabled:
            logger.warning(
                "Telegram devre disi — .env dosyasina TELEGRAM_CHAT_ID ekle."
            )

    def send(self, text: str) -> None:
        """Non-blocking gönderim. Çağıran await KULLANMAZ."""
        if not self.enabled:
            return
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                loop.create_task(self._post(text))
        except Exception as exc:
            logger.debug("Telegram task olusturulamadi: %s", exc)

    async def _post(self, text: str) -> None:
        """Gerçek HTTP isteği — arka planda çalışır, crash etmez."""
        try:
            async with aiohttp.ClientSession() as session:
                resp = await session.post(
                    self.url,
                    json={
                        "chat_id":                  self.chat_id,
                        "text":                     text,
                        "parse_mode":               "HTML",
                        "disable_web_page_preview": True,
                    },
                    timeout=aiohttp.ClientTimeout(total=15),
                )
                if resp.status != 200:
                    body = await resp.text()
                    logger.debug("Telegram hata %d: %s", resp.status, body[:120])
        except asyncio.CancelledError:
            pass   # Bot kapanıyorsa normal
        except Exception as exc:
            logger.debug("Telegram gonderim hatasi: %s", exc)


# ─────────────────────────────────────────────────────────────────────────────
# YARDIMCI: Adres kısaltma
# ─────────────────────────────────────────────────────────────────────────────

def _short(address: str) -> str:
    """0x1234...abcd formatı."""
    if len(address) >= 10:
        return address[:6] + "..." + address[-4:]
    return address


# ─────────────────────────────────────────────────────────────────────────────
# MESAJ ŞABLONLARI
# ─────────────────────────────────────────────────────────────────────────────

def fmt_scan_start(tag: str, borrower_count: int) -> str:
    return (
        f"🔄 <b>[{tag}-COLD] Tarama Basliyor...</b>\n"
        f"⏳ Bekleme suresi doldu, borclu listesi cekiliyor.\n"
        f"📋 Toplam adres: <b>{borrower_count:,}</b>\n"
        f"🕒 <b>Zaman:</b> {_now()}"
    )


def fmt_scan_done(tag: str, elapsed: float, hiz: float,
                  guvenli: int, zombi: int, kucuk: int,
                  karsiz: int, hot_add: int, firsat: int,
                  hot_list_size: int) -> str:
    return (
        f"✅ <b>[{tag}-COLD] Tarama Tamamlandi</b>\n"
        f"⏱ Sure: <b>{elapsed:.1f}s</b> | Hiz: <b>{hiz:.0f}/sn</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 Guvenli: {guvenli:,} | Zombi: {zombi:,} | Kucuk: {kucuk:,}\n"
        f"❌ Karsiz: {karsiz:,} | 🔴 Hot+: {hot_add:,} | 🎯 Firsat: {firsat:,}\n"
        f"🔥 Aktif Hot_list: <b>{hot_list_size:,}</b> adres\n"
        f"🕒 <b>Zaman:</b> {_now()}"
    )


def fmt_hot_list_add(tag: str, address: str, hf: float,
                     total_debt_usd: float, total_coll_usd: float,
                     debt_asset: str, debt_token_usd: float,
                     coll_asset: str, coll_token_usd: float,
                     pos_type: str, net_profit: float,
                     debt_cover_usd: float = 0.0) -> str:
    """Yeni hedef radarda bildirimi — Optimal Pair + Close Factor detaylı."""
    kar = f"+${net_profit:,.2f}" if net_profit >= 0 else f"-${abs(net_profit):,.2f}"
    return (
        f"⚠️ <b>[{tag}] TASFİYE HEDEFİ BULUNDU!</b>\n"
        f"👤 <b>Cüzdan:</b> <code>{address}</code>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 <b>Genel Durum:</b>\n"
        f"Health Factor: <code>{hf:.4f}</code>\n"
        f"Total Teminat: <code>${total_coll_usd:,.2f}</code>\n"
        f"Total Borç: <code>${total_debt_usd:,.2f}</code>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🎯 <b>Tasfiye Stratejisi (Optimal Pair):</b>\n"
        f"🔴 Kapatılacak Borç: <b>{debt_asset}</b> (Toplam Borcu: <code>${debt_token_usd:,.2f}</code>)\n"
        f"🟢 Alınacak Teminat: <b>{coll_asset}</b> (Toplam Teminatı: <code>${coll_token_usd:,.2f}</code>)\n"
        f"⚖️ Kapatılacak Miktar (%50 Kuralı): <code>${debt_cover_usd:,.2f}</code> değerinde {debt_asset}\n"
        f"🏷 Pozisyon Tipi: {pos_type}\n"
        f"📈 <b>Beklenen Net Kâr:</b> <code>{kar}</code>\n"
        f"⚙️ <b>Action:</b> Flashloan payload ({debt_asset} -> {coll_asset}) hazırlanıyor...\n"
        f"🕒 <b>Zaman:</b> {_now()}"
    )

def fmt_opportunity(tag: str, address: str, hf: float,
                    debt_usd: float, debt_asset: str,
                    coll_usd: float, coll_asset: str,
                    bonus_pct: float, flash_fee: float,
                    slippage: float, gas: float,
                    net_profit: float) -> str:
    return (
        f"🚨 <b>[{tag}] KÂRLI FIRSAT! HEDEFE KILITLENILDI!</b>\n"
        f"👤 <b>Cüzdan:</b> <code>{address}</code>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 <b>Health Factor:</b> <code>{hf:.6f}</code>\n"
        f"💸 <b>Kapatilacak Borc:</b> <code>${debt_usd:,.2f}</code> ({debt_asset})\n"
        f"🏦 <b>Alinacak Teminat:</b> <code>${coll_usd:,.2f}</code> "
        f"({coll_asset}) [Bonus: %{bonus_pct:.0f}]\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"➖ Flash Loan Fee: <code>-${flash_fee:.4f}</code>\n"
        f"➖ DEX Slippage: <code>-${slippage:.4f}</code>\n"
        f"➖ Gas: <code>-${gas:.4f}</code>\n"
        f"💰 <b>NET KÂR: <code>${net_profit:,.4f}</code></b>\n"
        f"🕒 <b>Zaman:</b> {_now()}"
    )


def fmt_autopsy_liquidated(
    tag: str, address: str,
    old_hf: float,   new_hf: float,
    old_debt: float, new_debt: float,
    old_coll: float, new_coll: float,
) -> str:
    """
    Durum 1: Başka bir bot/kullanıcı tasfiye etti / pozisyon kapandı.
    Borç büyük ölçüde buharlaştı ve teminat da düştü.
    """
    evaporated = max(0.0, old_debt - new_debt)
    debt_drop  = (evaporated / old_debt * 100) if old_debt > 0 else 0.0
    coll_drop  = ((old_coll - new_coll) / old_coll * 100) if old_coll > 0 else 0.0

    return (
        f"💀 <b>SİSTEM UYARISI: BAŞKASI VURDU / POZİSYON KAPATILDI</b> 💀\n"
        f"\n"
        f"💳 <b>Cüzdan:</b> <code>{address}</code>\n"
        f"📉 <b>Olay:</b> Tasfiye (Liquidation) veya Pozisyon Kapatma Tespit Edildi. "
        f"Sadece toz (dust) bakiye kaldi.\n"
        f"\n"
        f"📊 <b>Değer Değişimleri (Öncesi / Sonrasi):</b>\n"
        f"🩺 <b>HF Durumu:</b> <code>{old_hf:.4f}</code> ➡️ <code>{new_hf:.4f}</code>\n"
        f"💰 <b>Eski Borc:</b> <code>${old_debt:,.2f}</code>\n"
        f"💸 <b>Kalan Yeni Borc:</b> <code>${new_debt:,.2f}</code> "
        f"<i>(Toz Hesap — -%{debt_drop:.0f})</i>\n"
        f"🔥 <b>Silinen/Buharlaşan Borc:</b> <code>${evaporated:,.2f}</code>\n"
        f"🏦 <b>Eski Teminat:</b> <code>${old_coll:,.2f}</code> ➡️ "
        f"<code>${new_coll:,.2f}</code> <i>(-%{coll_drop:.0f})</i>\n"
        f"\n"
        f"⚙️ <b>Aksiyon:</b> Cüzdan hot_list'ten cikarildi ve ölü hesap olarak isaretlendi.\n"
        f"🕒 <b>Zaman:</b> {_now()}"
    )


def fmt_autopsy_repaid(
    tag: str, address: str,
    old_hf: float,   new_hf: float,
    old_debt: float, new_debt: float,
    old_coll: float, new_coll: float,
) -> str:
    """
    Durum 2: Kullanıcı borcunu ödedi veya teminat ekleyerek kurtuldu.
    Borç azaldı ama teminat görece sabit — organik kurtuluş.
    """
    debt_change = old_debt - new_debt
    debt_pct    = (debt_change / old_debt * 100) if old_debt > 0 else 0.0
    coll_change = new_coll - old_coll   # pozitif = teminat eklendi

    coll_line = (
        f"🛡️ <b>Teminat Değişimi:</b> <code>${old_coll:,.2f}</code> ➡️ "
        f"<code>${new_coll:,.2f}</code> "
        + (f"<i>(+${coll_change:,.2f} eklendi)</i>" if coll_change > 0 else "<i>(sabit)</i>")
    )

    return (
        f"🟢 <b>SİSTEM BİLGİSİ: CÜZDAN KURTARILDI / TEMİNAT EKLENDİ</b> 🟢\n"
        f"\n"
        f"💳 <b>Cüzdan:</b> <code>{address}</code>\n"
        f"📈 <b>Olay:</b> Kullanici borcunu odedi veya teminat ekleyerek "
        f"pozisyonunu guvenlige aldi.\n"
        f"\n"
        f"📊 <b>Değer Değişimleri (Öncesi / Sonrasi):</b>\n"
        f"🩺 <b>HF Durumu:</b> <code>{old_hf:.4f}</code> ➡️ <code>{new_hf:.4f}</code>\n"
        f"💰 <b>Eski Borc:</b> <code>${old_debt:,.2f}</code>\n"
        f"💳 <b>Güncel Borc:</b> <code>${new_debt:,.2f}</code> "
        f"<i>(-${debt_change:,.2f} / -%{debt_pct:.0f})</i>\n"
        f"{coll_line}\n"
        f"🛡️ <b>Durum:</b> HF Güvenli Seviyede (&gt; 1.05)\n"
        f"\n"
        f"⚙️ <b>Aksiyon:</b> Cüzdan takip listesinden (hot_list) guvenli listeye alindi.\n"
        f"🕒 <b>Zaman:</b> {_now()}"
    )