import os
import pytest
from src.scraper import BitcoinScraper

@pytest.fixture
def html_de_prueba():
    """Fixture que lee el contenido del archivo HTML mock simulado."""
    ruta_base = os.path.dirname(os.path.abspath(__file__))
    ruta_fixture = os.path.join(ruta_base, "fixtures", "sample_asset.html")
    
    with open(ruta_fixture, "r", encoding="utf-8") as archivo:
        return archivo.read()

def test_limpiar_precio_exitoso():
    """Valida la eliminacion correcta de simbolos, comas y espacios."""
    scraper = BitcoinScraper()
    assert scraper.limpiar_precio("$67,542.30") == 67542.30
    assert scraper.limpiar_precio("  $1,234.56  ") == 1234.56
    assert scraper.limpiar_precio("500") == 500.0

def test_limpiar_precio_vacio():
    """Valida la excepcion cuando la cadena de entrada esta vacia."""
    scraper = BitcoinScraper()
    with pytest.raises(ValueError):
        scraper.limpiar_precio("")

def test_extraer_precio_desde_html(html_de_prueba):
    """Prueba que el analizador de BeautifulSoup extraiga el valor correcto del HTML mock."""
    scraper = BitcoinScraper()
    precio_extraido = scraper.extraer_precio(html_de_prueba)
    assert precio_extraido == 67542.30

def test_extraer_precio_fallback():
    """Valida que el scraper aplique la estrategia de fallback si el selector principal cambia."""
    html_alterado = """
    <html>
        <head>
            <meta property="og:price:amount" content="63200.50">
        </head>
        <body>
            <div>El diseno ha cambiado por completo</div>
        </body>
    </html>
    """
    scraper = BitcoinScraper()
    precio_extraido = scraper.extraer_precio(html_alterado)
    assert precio_extraido == 63200.50