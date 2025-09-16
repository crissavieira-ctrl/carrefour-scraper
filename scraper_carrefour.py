# -- coding: utf-8 --
"""
Scraper Carrefour via JSON-LD (ld+json) para múltiplas cidades
- Modo: GitHub Actions + commit no repo
- Armazenamento: 1 Excel por cidade/mês (coluna por dia)
"""

import os, json, time, re
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
    # desliga imagens
    opts.add_experimental_option("prefs", {"profile.managed_default_content_settings.images": 2})
    driver = webdriver.Chrome(options=opts)
    driver.set_page_load_timeout(60)
    driver.implicitly_wait(2)
    return driver

# ======================================================
# 3) Definir CEP na UI (tenta múltiplos seletores comuns)
# ======================================================
def set_cep(driver: webdriver.Chrome, cep: str, timeout: int = 20):
    """
    Abre o modal de endereço, insere o CEP e confirma.
    Tenta seletores alternativos porque o site pode variar.
    """
    driver.get("https://mercado.carrefour.com.br/")
    wait = WebDriverWait(driver, timeout)

    # 3.1 abrir modal (tenta vários botões)
    opened = False
    open_btn_selectors = [
        ("css", 'button[aria-label*="CEP"]'),
        ("css", 'button[data-testid*="address"], button[data-testid*="location"]'),
        ("xpath", '//button[contains(., "Informe seu CEP")]'),
        ("xpath", '//button[contains(., "Alterar endereço") or contains(., "Trocar endereço")]'),
        ("css", 'button[aria-label*="endereço"]'),
    ]
    for how, sel in open_btn_selectors:
        try:
            elem = wait.until(
                EC.element_to_be_clickable(
                    (By.CSS_SELECTOR, sel) if how == "css" else (By.XPATH, sel)
                )
            )
            elem.click()
            opened = True
            break
        except Exception:
            continue
    if not opened:
        # plano B via JS
        driver.execute_script("""
            var btn = document.querySelector('button[aria-label*="CEP"], button[data-testid*="address"], button[data-testid*="location"], button[aria-label*="endereço"]');
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
        # fallback JS (inputs mascarados)
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
        driver.execute_script("""
            const b = [...document.querySelectorAll('button')].find(x => /confirmar|continuar|ok/i.test(x.textContent));
            if (b) b.click();
        """)
    time.sleep(2.5)  # respiro p/ recarregar preços

# ================================
# 4) Helpers para parse de JSON-LD
# ================================
_money_cleaner = re.compile(r"[^0-9,.\-]")

def to_float_price(x):
    if x is None:
        return 0.0
    # remove moeda/símbolos e troca vírgula por ponto
    s = _money_cleaner.sub("", str(x)).strip()
    s = s.replace(",", ".")
    try:
        return float(s)
    except Exception:
        return 0.0

def extract_price(offers):
    """
    Aceita dict ou list. Tenta price, lowPrice, priceSpecification.price.
    """
    if not offers:
        return 0.0
    cand = []
    if isinstance(offers, dict):
        cand.append(offers.get("price"))
        cand.append(offers.get("lowPrice"))
        spec = offers.get("priceSpecification") or {}
        if isinstance(spec, dict):
            cand.append(spec.get("price"))
    elif isinstance(offers, list):
        for it in offers:
            if not isinstance(it, dict):
                continue
            cand.extend([it.get("price"), it.get("lowPrice")])
            spec = it.get("priceSpecification") or {}
            if isinstance(spec, dict):
                cand.append(spec.get("price"))
    # primeira que virar float > 0
    for c in cand:
        v = to_float_price(c)
        if v > 0:
            return v
    # último recurso
    return to_float_price(cand[0] if cand else None)

def find_products_in_json(data):
    """
    Retorna lista de dicts do tipo Product presentes no JSON-LD,
    inclusive quando vem dentro de @graph.
    """
    found = []
    if isinstance(data, dict):
        if data.get("@type") == "Product":
            found.append(data)
        if "@graph" in data and isinstance(data["@graph"], list):
            for obj in data["@graph"]:
                if isinstance(obj, dict) and obj.get("@type") == "Product":
                    found.append(obj)
    elif isinstance(data, list):
        for obj in data:
            if isinstance(obj, dict) and obj.get("@type") == "Product":
                found.append(obj)
    return found

# =====================================
# 5) Scraper: lê JSON-LD do tipo Product
# =====================================
def scrape_product_via_json(url: str, driver: webdriver.Chrome) -> dict:
    try:
        driver.get(url)
        time.sleep(2)  # aguarda scripts carregarem

        tags = driver.find_elements(By.XPATH, '//script[@type="application/ld+json"]')
        for tag in tags:
            raw = tag.get_attribute("innerHTML")
            if not raw:
                continue

            # Alguns blocos têm múltiplos JSONs; tenta carregar direto,
            # se falhar, tenta dividir por fechamento "}" e reconstruir.
            try:
                data = json.loads(raw)
                candidates = find_products_in_json(data)
            except Exception:
                candidates = []
                # tentativa simples de fallback
                parts = re.split(r'\n(?=\s*[{[]")', raw)
                for p in parts:
                    try:
                        d = json.loads(p)
                        candidates.extend(find_products_in_json(d))
                    except Exception:
                        continue

            for obj in candidates:
                name = obj.get("name", "Não encontrado")
                price_float = extract_price(obj.get("offers"))
                return {"Nome do Produto": name, "Preço": float(price_float), "URL": url}
    except Exception as e:
        print(f"❌ Erro ao carregar {url}: {e}")

    # fallback (sempre retorna com 'Preço')
    return {"Nome do Produto": "Não encontrado", "Preço": 0.0, "URL": url}

# =========================
# 6) URLs (sua lista aqui)
# =========================
URLS = [
    # ------------------ Lista original ------------------
    'https://mercado.carrefour.com.br/arroz-branco-longofino-tipo-1-tio-joao-2kg-115657/p',
    'https://mercado.carrefour.com.br/feijao-carioca-tipo-1-kicaldo-1kg-466506/p',
    'https://mercado.carrefour.com.br/macarrao-de-semola-com-ovos-espaguete-8-adria-500g-4180372/p',
    'https://mercado.carrefour.com.br/farofa-de-mandioca-tradicional-yoki-400g-6582613/p',
    'https://mercado.carrefour.com.br/massa-para-pastel-discao-massa-leve-500g-841757/p',
    'https://mercado.carrefour.com.br/macarrao-instantaneo-nissin-sabor-galinha-caipira-85g-4814177/p',
    'https://mercado.carrefour.com.br/batata-monalisa-carrefour-aprox-600g-46922/p',
    'https://mercado.carrefour.com.br/pimentao-block-vermelho-trebeshi-150-g-5738458/p',
    'https://mercado.carrefour.com.br/tomate-carmem-carrefour-aprox-500g-262676/p',
    'https://mercado.carrefour.com.br/cebola-carrefour-aprox-500g-20621/p',
    'https://mercado.carrefour.com.br/cenoura-unico-1kg-5154669/p',
    'https://mercado.carrefour.com.br/acucar-refinado-uniao-1kg-197564/p',
    'https://mercado.carrefour.com.br/chocolate-ao-leite-com-amendoim-shot-165g-5790859/p',
    'https://mercado.carrefour.com.br/sorvete-napolitano-nestle-1-5-litros-8616043/p',
    'https://mercado.carrefour.com.br/achocolatado-em-po-nescau-550g-6409717/p',
    'https://mercado.carrefour.com.br/alface-lisa-carrefour-7745044/p',
    'https://mercado.carrefour.com.br/couve-flor-cledson-300-g-9560297/p',
    'https://mercado.carrefour.com.br/banana-nanica-fresca-organica-600g-210978/p',
    'https://mercado.carrefour.com.br/banana-prata-fischer-turma-da-monica-750g-9773711/p',
    'https://mercado.carrefour.com.br/limao-siciliano-carrefour-aprox-500g-63592/p',
    'https://mercado.carrefour.com.br/maca-gala-carrefour-aprox-600-g-10120/p',
    'https://mercado.carrefour.com.br/mamao-formosa-sabor-qualidade-aprox-16-kg-20524/p',
    'https://mercado.carrefour.com.br/manga-palmer-carrefour-aprox-600g-88919/p',
    'https://mercado.carrefour.com.br/melancia-premium-carrefour-aprox---8kg-194743/p',
    'https://mercado.carrefour.com.br/pera-willians-aprox-500g-39675/p',
    'https://mercado.carrefour.com.br/uva-escura-sem-semente-carrefour-500g-5141982/p',
    'https://mercado.carrefour.com.br/laranja-pera-carrefour-mercado-5-kg-6282032/p',
    'https://mercado.carrefour.com.br/bisteca-suina-congelada-sadia-1-kg-209864/p',
    'https://mercado.carrefour.com.br/contra-file-swift-mais-aprox-1-5kg-295906/p',
    'https://mercado.carrefour.com.br/coxao-mole-fracionado-a-vacuo-aprox--1-3-kg-18295/p',
    'https://mercado.carrefour.com.br/alcatra-bovina-carrefour-aproximadamente-400-g-21962/p',
    'https://mercado.carrefour.com.br/patinho-fracionado-a-vacuo-500g-18325/p',
    'https://mercado.carrefour.com.br/lagarto-swift-mais-aprox-15kg-295914/p',
    'https://mercado.carrefour.com.br/paleta-bovina-a-vacuo-500gnao-reativarcodigo-de-compra-20745/p',
    'https://mercado.carrefour.com.br/acem-em-pedacos-carrefour-aproximadamente-500-g-158828/p',
    'https://mercado.carrefour.com.br/costela-minga-bovina-cong-aprox-2kg-224006/p',
    'https://mercado.carrefour.com.br/camarao-descascado-cozido-36-40-celm-400-g-5939747/p',
    'https://mercado.carrefour.com.br/posta-cacao-congelado-buona-pesca-500-g-6311059/p',
    'https://mercado.carrefour.com.br/file-de-merluza-congelado-planalto-500-g-6323774/p',
    'https://mercado.carrefour.com.br/file-de-pescada-sem-espinha-swift-500-g-5457297/p',
    'https://mercado.carrefour.com.br/file-de-tilapia-fresco-carrefour-500-g-98930/p',
    'https://mercado.carrefour.com.br/presunto-cozido-sem-capa-fatiado-aurora-aproximadamente-200-g-49450/p',
    'https://mercado.carrefour.com.br/salsicha-hot-dog-resfriada-aurora-aproximadamente-500-g-49352/p',
    'https://mercado.carrefour.com.br/linguica-toscana-swift-700-g-5600812/p',
    'https://mercado.carrefour.com.br/mortadela-defumada-sadia-280g-5447045/p',
    'https://mercado.carrefour.com.br/queijo-minas-frescal-aurora-450-g-6264693/p',
    'https://mercado.carrefour.com.br/queijo-coalho-bom-leite-500-g-4305054/p',
    'https://mercado.carrefour.com.br/leite-uht-integral-piratininga-1-l-665017/p',
    'https://mercado.carrefour.com.br/iogurte-natural-tradicional-batavo-170g-5150439/p',
    'https://mercado.carrefour.com.br/manteiga-com-sal-aviacao-200-g-10010/p',
    'https://mercado.carrefour.com.br/creme-de-leite-ultrapasteurizado-itambe-200-g-5988921/p',
    'https://mercado.carrefour.com.br/requeijao-cremoso-aviacao-tradicional-220-g-10000/p',
    'https://mercado.carrefour.com.br/acucar-cristal-carrefour-1kg-5147300/p',
    'https://mercado.carrefour.com.br/mel-com-cacau-e-avela-400-g-4510146/p',
    'https://mercado.carrefour.com.br/geleia-de-goiaba-selecoes-c-pedacos-260-g-1280815/p',
    'https://mercado.carrefour.com.br/suco-de-uva-integral-maric-1-l-3538256/p',
    'https://mercado.carrefour.com.br/vinho-tinto-fino-seco-cabernet-sauvignon-pergola-750ml-1521709/p',
    'https://mercado.carrefour.com.br/whisky-red-label-johnnie-walker-1-litro-2719/p',
    'https://mercado.carrefour.com.br/refrigerante-coca-cola-sabor-cola-1-5-l-11087/p',
    'https://mercado.carrefour.com.br/cafe-torrado-e-moido-extraforte-melitta-500g-271203/p',
    'https://mercado.carrefour.com.br/farinha-de-trigo-dona-benta-tradicional-1kg-196416/p',
    'https://mercado.carrefour.com.br/azeite-extravirgem-portugues-oliveira-da-serra-500-ml-4526108/p',
    'https://mercado.carrefour.com.br/oleo-de-soja-soya-900ml-482616/p',
    'https://mercado.carrefour.com.br/margarina-qualy-com-sal-250g-4815618/p',
    'https://mercado.carrefour.com.br/arroz-branco-longofino-tipo-1-tio-joao-1kg-115658/p',
    'https://mercado.carrefour.com.br/feijao-preto-tipo-1-kicaldo-1kg-466510/p',

    # ------------------ Itens adicionais ------------------
    # Arroz
    'https://mercado.carrefour.com.br/arroz-branco-longo-fino-tipo-1-meu-biju-1kg-4956435/p',
    'https://mercado.carrefour.com.br/arroz-branco-carrefour-classic-olimpiadas-1kg-3433455/p',
    'https://mercado.carrefour.com.br/arroz-branco-longofino-tipo-1-prato-fino-1-kg-3142248/p',
    'https://mercado.carrefour.com.br/arroz-branco-longofino-tipo-1-camil-todo-dia-1kg-1336118/p',
    'https://mercado.carrefour.com.br/arroz-branco-longofino-tipo-1-tio-joao-1-kg-387606/p',
    'https://mercado.carrefour.com.br/arroz-parboilizado-longo-fino-tipo-1-carrefour-1kg-6677711/p',
    'https://mercado.carrefour.com.br/arroz-parboilizado-longo-fino-tipo-1-tio-joao-1-kg-3136400/p',
    'https://mercado.carrefour.com.br/arroz-parboilizado-longo-fino-tipo-1-prato-fino-1-kg-7043236/p',

    # Pão francês
    'https://mercado.carrefour.com.br/pao-frances-carrefour-aprox-110g-168076/p',
    'https://mercado.carrefour.com.br/busca/pao%20frances',

    # Leite longa vida
    'https://mercado.carrefour.com.br/leite-desnatado-piracanjuba-1-litro-3371697/p',
    'https://mercado.carrefour.com.br/leite-desnatado-uht-molico-1-l-6083900/p',
    'https://mercado.carrefour.com.br/leite-desnatado-uht-tipo-a-leitissimo-1-litro-9682953/p',
    'https://mercado.carrefour.com.br/leite-semidesnatado-liquido-parmalat-1-litro-5254337/p',
    'https://mercado.carrefour.com.br/leite-semidesnatado-piracanjuba-1-litro-7863756/p',
    'https://mercado.carrefour.com.br/leite-semidesnatado-uht-goiasminas-italac-1-litro-8819530/p',
    'https://mercado.carrefour.com.br/leite-uht-integral-carrefour-classic-1l-3218023/p',
    'https://mercado.carrefour.com.br/leite-sem-lactose-integral-uht-italac-1-litro-5823048/p',

    # Biscoito
    'https://mercado.carrefour.com.br/biscoito-com-chocolate-chocobiscuit-nestle-ao-leite-78g-3485935/p',
    'https://mercado.carrefour.com.br/biscoito-amanteigado-chocolate-e-doce-de-leite-carrefour-100-g-6226213/p',
    'https://mercado.carrefour.com.br/busca/biscoito%20doce',
    'https://mercado.carrefour.com.br/biscoito-de-polvilho-doce-carrefour-200g-7738714/p',
    'https://mercado.carrefour.com.br/biscoito-salgado-club-social-original-multipack-144g-9923357/p',
    'https://mercado.carrefour.com.br/biscoito-de-polvilho-salgado-carrefour-200g-5570417/p',
    'https://mercado.carrefour.com.br/biscoito-salgado-cream-cracker-integral-piraque-215g-3179591/p',

    # Refrigerante e água mineral
    'https://mercado.carrefour.com.br/refrigerante-guarana-antarctica-garrafa-2l-156396/p',
    'https://mercado.carrefour.com.br/refrigerante-cocacola-garrafa-2-l-5761719/p',
    'https://mercado.carrefour.com.br/refrigerante-fanta-laranja-2l-157201/p',
    'https://mercado.carrefour.com.br/agua-mineral-sem-gas-nestle-pureza-vital-15-litros-7026099/p',
    'https://mercado.carrefour.com.br/agua-mineral-crystal-sem-gas-15l-8812128/p',
    'https://mercado.carrefour.com.br/agua-mineral-sem-gas-minalba-15-litros-708941/p',
    'https://mercado.carrefour.com.br/agua-mineral-sem-gas-frescca-15-litros-4928784/p',

    # Frango inteiro
    'https://mercado.carrefour.com.br/frango-inteiro-temperado-seara-assa-facil-aprox-19kg-170739/p',
    'https://mercado.carrefour.com.br/frango-inteiro-swift-aprox-25-kg-213519/p',

    # Café moído
    'https://mercado.carrefour.com.br/cafe-torrado-e-moido-a-vacuo-tradicional-pilao-500g-7515758/p',
    'https://mercado.carrefour.com.br/busca/cafe%20moido',
    'https://mercado.carrefour.com.br/cafe-torrado-e-moido-do-ponto-exportacao-vacuo-500-g-4416090/p',
    'https://mercado.carrefour.com.br/cafe-torrado-e-moido-a-vacuo-bom-jesus-500g-8343527/p',
    'https://mercado.carrefour.com.br/cafe-torrado-e-moido-3-coracoes-cerrado-mineiro-250-g-6127002/p',
    'https://mercado.carrefour.com.br/cafe-starbucks-house-blend-torrado-e-moido-torra-media-250g-5688396/p',

    # Cerveja
    'https://mercado.carrefour.com.br/cerveja-heineken-garrafa-600ml-7941234/p',
    'https://mercado.carrefour.com.br/cerveja-baden-baden-golden-ale-garrafa-600ml-7948190/p',
    'https://mercado.carrefour.com.br/cerveja-brahma-duplo-malte-puro-malte-350ml-lata-6643426/p',
    'https://mercado.carrefour.com.br/cerveja-budweiser-american-lager-lata-269-ml-9704698/p',
    'https://mercado.carrefour.com.br/cerveja-pilsen-original-lata-269ml-6418724/p',
    'https://mercado.carrefour.com.br/cerveja-original-pilsen-350ml-lata-5699193/p',
    'https://mercado.carrefour.com.br/cerveja-amstel-lager-lata-sleek-350ml-3180107/p',
    'https://mercado.carrefour.com.br/cerveja-heineken-lata-269ml-6688802/p',

    # Costela
    'https://mercado.carrefour.com.br/costela-bovina-janela-congelada-aprox-1-8kg-224014/p',
    'https://mercado.carrefour.com.br/busca/costela?page=1',
    'https://mercado.carrefour.com.br/costela-de-cordeiro-a-vacuo-28738/p',

    # Queijo
    'https://mercado.carrefour.com.br/queijo-mussarela-fatiado-president-150g-8613966/p',
    'https://mercado.carrefour.com.br/queijo-fatiado-sabor-mussarela-polenghi-144g-7413394/p',
    'https://mercado.carrefour.com.br/queijo-mussarela-fatiado-carrefour-aproximadamente-200-g-25585/p',
    'https://mercado.carrefour.com.br/queijo-mussarela-importado-fatiado-aprox-200g-149225/p',
    'https://mercado.carrefour.com.br/queijo-mussarela-fatiado-mandaka-com-150-g-6709206/p',
    'https://mercado.carrefour.com.br/queijo-prato-fatiado-president-150g-8614008/p',
    'https://mercado.carrefour.com.br/queijo-prato-fatiado-tirolez-150g-5033799/p',

    # Linguiça
    'https://mercado.carrefour.com.br/busca/lingui%C3%A7a',
    'https://mercado.carrefour.com.br/linguica-toscana-grossa-auora-aprox--700g-21113/p',
    'https://mercado.carrefour.com.br/linguica-toscana-sadia-700g-3213242/p',
    'https://mercado.carrefour.com.br/linguica-toscana-swift-700-g-5600812/p',
    'https://mercado.carrefour.com.br/busca/lingui%C3%A7a?page=3',

    # Leite em pó
    'https://mercado.carrefour.com.br/leite-em-po-molico-desnatado-lata-280g-9442405/p',
    'https://mercado.carrefour.com.br/leite-em-po-integral-italac-200g-7680198/p',
    'https://mercado.carrefour.com.br/leite-em-po-ninho-adulto-lata-350g-3428877/p',
    'https://mercado.carrefour.com.br/leite-desnatado-em-po-instantaneo-italac-280g-8669937/p',

    # Ovo de galinha
    'https://mercado.carrefour.com.br/ovos-brancos-carrefour-20-unidades-5286387/p',
    'https://mercado.carrefour.com.br/ovo-branco-grande-ac-planalto-ovos-bandeja-com-20-6206310/p',
    'https://mercado.carrefour.com.br/ovos-vermelhos-carrefour-20-unidades-8453624/p',
    'https://mercado.carrefour.com.br/ovo-vermelho-grande-mantiqueira-happy-eggs-com-20-unidades-6403603/p',
    'https://mercado.carrefour.com.br/ovo-branco-grande-mantiqueira-happy-eggs-com-20-unidades-6403565/p',
    'https://mercado.carrefour.com.br/ovo-caipira-grande-organicos-raiar-com-20-unidades-3050050/p',

    # Óleo de soja
    'https://mercado.carrefour.com.br/oleo-de-soja-confiare-900ml-3731243/p',
    'https://mercado.carrefour.com.br/oleo-de-soja-soya-900ml-141836/p',
    'https://mercado.carrefour.com.br/oleo-de-soja-vitaliv-garrafa-900-ml-6473563/p'
]

# ===========================================
# 7) Persistência: salva por cidade/mês/dia
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
# 8) Execução principal
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
                    rec = scrape_product_via_json(url, driver)
                    # garante as chaves esperadas
                    if "Preço" not in rec:
                        rec["Preço"] = 0.0
                    if "Nome do Produto" not in rec:
                        rec["Nome do Produto"] = "Não encontrado"
                    if "URL" not in rec:
                        rec["URL"] = url
                    registros.append(rec)
                except Exception as e:
                    registros.append({"Nome do Produto": "Erro", "Preço": 0.0, "URL": url, "Erro": str(e)})
                time.sleep(0.8)

            df_total = pd.DataFrame(registros)

            # Garante colunas mesmo que vazio
            for col in ["Nome do Produto", "Preço", "URL"]:
                if col not in df_total.columns:
                    df_total[col] = [] if col != "Preço" else []

            if "Preço" in df_total.columns:
                df_ok  = df_total[df_total["Preço"].fillna(0) > 0][["Nome do Produto", "Preço", "URL"]].copy()
                df_err = df_total[df_total["Preço"].fillna(0) <= 0].copy()
            else:
                # fallback extremo (não deveria ocorrer, mas evita KeyError)
                df_ok  = pd.DataFrame(columns=["Nome do Produto", "Preço", "URL"])
                df_err = df_total.copy()

            # Debug útil no CI
            print(f"[{cidade_slug}] Capturados: {len(df_total)} | OK: {len(df_ok)} | Erros/zeros: {len(df_err)}")
            if not df_ok.empty:
                print(df_ok.head(3).to_string(index=False))

            salvar_excel_cidade(cidade_slug, df_ok, df_err)

    finally:
        driver.quit()

if __name__ == "__main__":
    main()


