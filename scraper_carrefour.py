# -- coding: utf-8 --
"""
Scraper Carrefour via JSON-LD (ld+json) para múltiplas cidades
- Modo: GitHub Actions + commit no repo
- Armazenamento: 1 Excel por cidade/mês (coluna por dia)
"""

import os, json, time
from datetime import datetime
import pandas as pd

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# =========================
# 0) Cidades e CEPs-alvo
# =========================
CIDADES = {
    "Belo_Horizonte": "30110-002",
    "Rio_de_Janeiro": "20010-000",
    "Salvador": "40020-000",
    "Curitiba": "80010-000",
    "Porto_Alegre": "90010-150",
    "Belem": "66010-000",
    "Recife": "50010-040",
}

# =========================
# 1) Paths e nomes mensais
# =========================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
os.makedirs(DATA_DIR, exist_ok=True)

today = datetime.now()
STAMP_DAY = today.strftime("%Y%m%d")       # -> coluna diária (Preço_YYYYMMDD)
STAMP_MONTH = today.strftime("%Y-%m")      # -> arquivo do mês (precos_carrefour_YYYY-MM.xlsx)
COLUNA_DIA = f"Preço_{STAMP_DAY}"

# =========================================
# 2) Driver (headless — ideal para Actions)
# =========================================
def build_driver(headless: bool = True):
    opts = webdriver.ChromeOptions()
    if headless:
        opts.add_argument("--headless=new")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    opts.page_load_strategy = "eager"  # acelera
    opts.add_experimental_option("prefs", {"profile.managed_default_content_settings.images": 2})
    driver = webdriver.Chrome(options=opts)
    driver.set_page_load_timeout(60)
    driver.implicitly_wait(2)
    return driver

# ======================================================
# 3) Definir CEP na UI (tenta múltiplos seletores comuns)
# ======================================================
def set_cep(driver: webdriver.Chrome, cep: str, timeout: int = 15):
    """
    Abre o modal de endereço, insere o CEP e confirma.
    Tenta seletores alternativos porque o site pode variar.
    """
    # abrir qualquer página do Carrefour Mercado (home é leve)
    driver.get("https://mercado.carrefour.com.br/")
    wait = WebDriverWait(driver, timeout)

    # 3.1 abrir modal (tenta vários botões)
    opened = False
    open_btn_selectors = [
        ('css', 'button[aria-label*="CEP"]'),
        ('css', 'button[data-testid*="address"], button[data-testid*="location"]'),
        ('xpath', '//button[contains(., "Informe seu CEP")]'),
        ('xpath', '//button[contains(., "Alterar endereço") or contains(., "Trocar endereço")]'),
    ]
    for how, sel in open_btn_selectors:
        try:
            elem = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, sel))) if how == 'css' \
                else wait.until(EC.element_to_be_clickable((By.XPATH, sel)))
            elem.click()
            opened = True
            break
        except Exception:
            continue
    if not opened:
        # plano B: abre o dropdown via JS se existir gatilho
        driver.execute_script("""
            var btn = document.querySelector('button[aria-label*="CEP"], button[data-testid*="address"], button[data-testid*="location"]');
            if (btn) btn.click();
        """)
        time.sleep(1)

    # 3.2 inserir CEP (tenta names/ids comuns)
    input_locators = [
        (By.CSS_SELECTOR, 'input[name*="cep"], input[id*="cep"], input[name*="zip"], input[id*="zip"]'),
        (By.XPATH, '//input[@type="text" and (contains(@name,"cep") or contains(@id,"cep") or contains(@name,"zip") or contains(@id,"zip"))]'),
    ]
    cep_input = None
    for by, sel in input_locators:
        try:
            cep_input = wait.until(EC.visibility_of_element_located((by, sel)))
            break
        except Exception:
            continue

    if cep_input is None:
        # fallback: tenta preencher via JS (alguns inputs são mascarados)
        driver.execute_script("""
            const i = document.querySelector('input[name*="cep"], input[id*="cep"], input[name*="zip"], input[id*="zip"]');
            if (i) { i.value = arguments[0]; i.dispatchEvent(new Event('input', {bubbles:true})); }
        """, cep)
    else:
        cep_input.clear()
        cep_input.send_keys(cep)
        time.sleep(0.6)

    # 3.3 confirmar endereço (tenta botões comuns)
    confirm_selectors = [
        (By.CSS_SELECTOR, 'button[type="submit"]'),
        (By.XPATH, '//button[contains(., "Confirmar") or contains(., "Continuar") or contains(., "OK")]'),
    ]
    clicked = False
    for by, sel in confirm_selectors:
        try:
            btn = wait.until(EC.element_to_be_clickable((by, sel)))
            btn.click()
            clicked = True
            break
        except Exception:
            continue
    if not clicked:
        # último recurso
        driver.execute_script("""
            const b = [...document.querySelectorAll('button')].find(x => /confirmar|continuar|ok/i.test(x.textContent));
            if (b) b.click();
        """)
    # dá um respiro p/ recarregar preços
    time.sleep(2.5)

# =====================================
# 4) Scraper: lê JSON-LD do tipo Product
# =====================================
def scrape_product_via_json(url: str, driver: webdriver.Chrome) -> dict:
    driver.get(url)
    time.sleep(2)  # aguarda scripts carregarem

    try:
        tags = driver.find_elements(By.XPATH, '//script[@type="application/ld+json"]')
        for tag in tags:
            raw = tag.get_attribute("innerHTML")
            if not raw:
                continue

            try:
                data = json.loads(raw)
            except Exception:
                continue

            objs = data if isinstance(data, list) else [data]
            for obj in objs:
                if isinstance(obj, dict) and obj.get("@type") == "Product":
                    name = obj.get("name", "Não encontrado")

                    # offers pode ser dict ou lista
                    offers = obj.get("offers", {})
                    price = None
                    if isinstance(offers, dict):
                        price = offers.get("price")
                    elif isinstance(offers, list) and offers:
                        price = (offers[0] or {}).get("price")

                    try:
                        price_float = float(str(price).replace(",", "."))
                    except Exception:
                        price_float = 0.0

                    return {"Nome do Produto": name, "Preço": price_float, "URL": url}

    except Exception as e:
        print("❌ Erro no parsing JSON-LD:", e)

    return {"Nome do Produto": "Não encontrado", "Preço": 0.0, "URL": url}

# =========================
# 5) URLs (sua lista aqui)
# =========================
URLS = [
    # ... (mantenha sua lista atual de URLs)
]

# ===========================================
# 6) Persistência: salva por cidade/mês/dia
# ===========================================
def salvar_excel_cidade(cidade_slug: str, df_ok: pd.DataFrame, df_err: pd.DataFrame):
    city_dir = os.path.join(DATA_DIR, cidade_slug)
    os.makedirs(city_dir, exist_ok=True)

    arq_mensal = os.path.join(city_dir, f"precos_carrefour_{STAMP_MONTH}.xlsx")
    arq_erros  = os.path.join(city_dir, f"erros_carrefour_{STAMP_MONTH}.xlsx")

    # ---- Excel mensal: aba "Precos" com coluna diária ----
    if not df_ok.empty:
        if os.path.exists(arq_mensal):
            base = pd.read_excel(arq_mensal, sheet_name="Precos")
            if "Nome do Produto" not in base.columns:
                base["Nome do Produto"] = df_ok["Nome do Produto"]
            base = base.merge(df_ok, on="Nome do Produto", how="outer")
            if "Preço" in base.columns:
                base[COLUNA_DIA] = base.pop("Preço")
        else:
            base = df_ok.rename(columns={"Preço": COLUNA_DIA})

        with pd.ExcelWriter(arq_mensal, engine="openpyxl", mode="w") as w:
            base.to_excel(w, index=False, sheet_name="Precos")
        print(f"📁 [{cidade_slug}] Atualizado: {arq_mensal} (coluna {COLUNA_DIA})")
    else:
        print(f"⚠️ [{cidade_slug}] Nenhum preço válido hoje.")

    # ---- Log de erros do mês ----
    if not df_err.empty:
        df_err = df_err.copy()
        df_err["Data"] = today.strftime("%Y-%m-%d")
        if os.path.exists(arq_erros):
            be = pd.read_excel(arq_erros)
            be = pd.concat([be, df_err], ignore_index=True)
        else:
            be = df_err
        be.to_excel(arq_erros, index=False)
        print(f"⚠️ [{cidade_slug}] Erros/zeros salvos: {arq_erros}")
    else:
        print(f"✅ [{cidade_slug}] Sem erros hoje.")

# =========================
# 7) Execução principal
# =========================
def main():
    driver = build_driver(headless=True)

    try:
        for cidade_slug, cep in CIDADES.items():
            print(f"\n================ {cidade_slug} (CEP {cep}) ================")
            # define CEP da cidade
            set_cep(driver, cep)
            time.sleep(1.0)

            registros = []
            for url in URLS:
                try:
                    registros.append(scrape_product_via_json(url, driver))
                except Exception as e:
                    registros.append({"Nome do Produto": "Erro", "Preço": 0.0, "URL": url, "Erro": str(e)})
                time.sleep(0.8)

            df_total = pd.DataFrame(registros)
            df_ok  = df_total[df_total["Preço"] > 0][["Nome do Produto", "Preço", "URL"]].copy()
            df_err = df_total[df_total["Preço"] <= 0].copy()

            salvar_excel_cidade(cidade_slug, df_ok, df_err)

    finally:
        driver.quit()

if __name__ == "__main__":
    main()

