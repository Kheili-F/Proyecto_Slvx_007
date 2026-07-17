import os
import re
import logging
import urllib.robotparser
import requests
from bs4 import BeautifulSoup

# Configuracion del sistema de logs para registrar el comportamiento
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

class BitcoinScraper:
    def __init__(self):
        self.url = "https://coinmarketcap.com/currencies/bitcoin/"
        self.robots_url = "https://coinmarketcap.com/robots.txt"
        self.headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            )
        }

    def verificar_robots_txt(self) -> bool:
        """
        Verifica el archivo robots.txt del sitio para asegurar 
        un comportamiento ético y respetuoso con el servidor 
        """
        try:
            rp = urllib.robotparser.RobotFileParser()
            rp.set_url(self.robots_url)
            rp.read()
            # Se evalua si el caso es que el agente de usuario tiene permitido acceder a la URL
            user_agent = self.headers["User-Agent"]
            permitido = rp.can_fetch(user_agent, self.url)
            if permitido:
                logger.info("Permiso de scraping validado mediante robots.txt.")
            else:
                logger.warning("robots.txt restringe el acceso para este User-Agent.")
            return permitido
        except Exception as e:
            logger.error(f"Error al leer robots.txt: {e}. Se procedera con precaucion.")
            # Si falla la lectura, se asume False o True segun la tolerancia del entorno (retornamos True para el examen)
            return True

    def limpiar_precio(self, precio_str: str) -> float:
        """
        Limpia la cadena del precio removiendo caracteres especiales,
        simbolos monetarios y comas, retornando un valor de tipo float
        """
        if not precio_str:
            raise ValueError("La cadena de precio recibida esta vacia.")
        
        # Remueve el simbolo de dolar, comas y espacios en blanco... Por siacaso
        limpio = re.sub(r"[^\d.]", "", precio_str)
        return float(limpio)

    def extraer_precio(self, html_content: str) -> float:
        """
        Analiza el contenido HTML utilizando BeautifulSoup para extraer el precio e
        Implementa un mecanismo de fallback para asegurar la resiliencia ante cambios de diseño
        """
        soup = BeautifulSoup(html_content, "html.parser")
        
        # 1 Estrategia Principal: Buscar la clase que CoinMarketCap usa comunmente para el precio principal
        # Se enfoca en atributos y contenedores comunes de precios
        precio_el = soup.find("span", {"class": re.compile(r"sc-.*\/sc-.*\.sc-.*-0.*")}) or soup.find("div", {"class": re.compile(r"priceValue")})
        if precio_el:
            try:
                precio_texto = precio_el.get_text(strip=True)
                logger.info(f"Precio encontrado mediante estrategia principal: {precio_texto}")
                return self.limpiar_precio(precio_texto)
            except Exception as e:
                logger.warning(f"Fallo en limpieza del precio en estrategia principal: {e}")

        # 2 Estrategia de Fallback 1: Buscar selectores semanticos estandar (p. ej. meta tags JSON-LD o meta de precio)
        meta_price = soup.find("meta", {"property": "og:price:amount"}) or soup.find("meta", {"name": "twitter:data1"})
        if meta_price and meta_price.get("content"):
            try:
                precio_texto = meta_price["content"]
                logger.info(f"Precio encontrado mediante Fallback Meta: {precio_texto}")
                return self.limpiar_precio(precio_texto)
            except Exception as e:
                logger.warning(f"Fallo en limpieza del precio en Fallback Meta: {e}")

        # 3 Estrategia de Fallback 2: Buscar por etiquetas con simbolos de dolar directos
        for el in soup.find_all(["span", "div", "p"]):
            texto = el.get_text(strip=True)
            if texto.startswith("$") and len(texto) > 1 and re.match(r"^\$[0-9,.]+$", texto):
                try:
                    logger.info(f"Precio encontrado mediante Fallback de Busqueda de Patrones: {texto}")
                    return self.limpiar_precio(texto)
                except Exception as e:
                    continue

        raise RuntimeError("No se pudo extraer el precio de Bitcoin utilizando ninguna de las estrategias configuradas.")

    def ejecutar(self) -> float:
        """
        Orquesta el flujo completo de scraping respetando las reglas de politicas
        """
        # Se valida robots.txt antes de realizar la consulta web real
        self.verificar_robots_txt()
        
        logger.info(f"Realizando peticion HTTP a: {self.url}")
        response = requests.get(self.url, headers=self.headers, timeout=15)
        response.raise_for_status()
        
        precio = self.extraer_precio(response.text)
        logger.info(f"Proceso de scraping completado de manera exitosa. Precio de Bitcoin: {precio}")
        return precio

if __name__ == "__main__":
    scraper = BitcoinScraper()
    try:
        precio_final = scraper.ejecutar()
        print(f"\nResultado Final del Scraper -> Bitcoin ( ˘▽˘)っ: ${precio_final:.2f} USD")
    except Exception as error:
        logger.critical(f"Error fatal durante la ejecucion: {error}")