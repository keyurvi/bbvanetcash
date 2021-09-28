#! /usr/bin/python3
import logging
import sys
from datetime import datetime
from logging.handlers import RotatingFileHandler
from multiprocessing import Process
from re import sub
from time import time, sleep
import base64

from azure.storage.blob import BlobServiceClient
from azure.storage.blob import ContentSettings
from flask import Flask, request
from requests import post
from selenium import webdriver

app = Flask(__name__)


@app.route('/', methods=['GET', 'POST'])
def mainworker():
    if request.method == 'GET':
        return "Spider is working OK."
    elif request.method == 'POST':
        utc_now = lambda: datetime.utcnow()
        utc_now = sub(":|\.| ", "_", str(utc_now()))
        spidername = "bbvanetcash"
        filename = f"{spidername}_{utc_now}.json"
        p = Process(target=saveJSON, args=(filename,))
        p.daemon = True
        p.start()
        return filename


def saveJSON(filename):
    bsc = BlobServiceClient(account_url="https://matikafilestorage.blob.core.windows.net/",
                            credential="ZHefXyWtrOJyAN2mNYKVxC0vYocITMdAUhOeW5JQzTCz/KqtDHgz4hsUpiVm6ViGFkBvuj3WkncABrnYFCFGnw==")
    blob_client = bsc.get_blob_client("spiderjobs", filename)

    password = str(request.headers.get("clave"))
    company_code = str(request.headers.get("rutempresa1"))
    user_code = str(request.headers.get("rutusuario1"))
    startdate = str(request.headers.get("startdate"))
    start_date = datetime.strptime(startdate, "%d-%m-%Y").strftime('%d/%m/%Y')
    enddate = str(request.headers.get("enddate"))
    end_date = datetime.strptime(enddate, "%d-%m-%Y").strftime('%d/%m/%Y')

    request_data = {k.lower(): v for k, v in request.headers.items()}


    start_date_split = start_date.split('/')
    start_date_date = start_date_split[0]
    start_date_month = start_date_split[1]
    start_date_year = start_date_split[2]

    end_date_split = end_date.split('/')
    end_date_date = end_date_split[0]
    end_date_month = end_date_split[1]
    end_date_year = end_date_split[2]

    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--start-maximized")
    driver = webdriver.Chrome(chrome_options=chrome_options)

    driver.get('https://www.bbvanetcash.pe/local_pibee/KDPOSolicitarCredenciales_es.html')
    sleep(5)
    timestart = time()
    try:

        try:
            driver.find_element_by_xpath('//*[@id="empresa"]').send_keys(company_code)
            driver.find_element_by_xpath('//*[@id="usuario"]').send_keys(user_code)
            driver.find_element_by_xpath('//*[@name="eai_password"]').send_keys(password)
            sleep(3)
            # driver.find_element_by_xpath('//*[@title="Ingresar"]').send_keys("\n")
            driver.find_element_by_xpath('//*[@title="Ingresar"]').click()
            sleep(5)
            print('SuccessFully Log In....')
            try:
                driver.find_element_by_xpath('//*[@id="custom-lightbox-close-img"]').click()
            except Exception as e:
                print('advertise not found')
            driver.find_elements_by_xpath("//a[contains(text(),'INFORMACIÓN DE CUENTAS')]")[1].click()
            sleep(1)
            driver.find_elements_by_xpath("//a[contains(text(),'Movimientos del Día')]")[1].click()
            sleep(1)
            driver.find_element_by_xpath("//a[contains(text(),'Consulta de Movimiento')]").click()
            sleep(10)
            output_res = {}
            all_data = []
            try:
                driver.switch_to.frame(driver.find_element_by_xpath('//*[@id="kyop-central-load-area"]'))
                sleep(1)
                account_numbers = driver.find_elements_by_xpath('//*[@name="AsuntoPropio"]/option')
                i = 0
                for account_number in account_numbers:
                    print('Fetching data from account :' + str(i + 1))
                    try:
                        if i > 0:
                            driver.switch_to.default_content()
                            driver.find_element_by_xpath("//a[contains(text(),'Consulta de Movimiento')]").click()
                            sleep(7)
                            driver.switch_to.frame(driver.find_element_by_xpath('//*[@id="kyop-central-load-area"]'))
                            sleep(3)
                            driver.find_element_by_xpath('//*[@name="AsuntoPropio"]').click()
                            sleep(1)
                            driver.find_elements_by_xpath('//*[@name="AsuntoPropio"]/option')[i].click()
                    except Exception as e:
                        print(str(e))
                    driver.find_element_by_xpath('//*[@id="radio1"]/parent::label').click()
                    sleep(1)
                    driver.find_element_by_xpath('//*[@name="DiaDesde"]').send_keys(start_date_date)
                    driver.find_element_by_xpath('//*[@name="MesDesde"]').send_keys(start_date_month)
                    driver.find_element_by_xpath('//*[@name="AnioDesde"]').send_keys(start_date_year)
                    driver.find_element_by_xpath('//*[@name="DiaHasta"]').send_keys(end_date_date)
                    driver.find_element_by_xpath('//*[@name="MesHasta"]').send_keys(end_date_month)
                    driver.find_element_by_xpath('//*[@name="AnioHasta"]').send_keys(end_date_year)
                    sleep(1)
                    driver.find_element_by_xpath('//*[@value="Consultar"]').click()
                    sleep(5)
                    if 'El criterio de Consulta de Movimientos seleccionado no posee resultados. Inténtelo nuevamente' not in driver.page_source:
                        odd_rows = driver.find_elements_by_xpath('//*[@class="odd"]')
                        even_rows = driver.find_elements_by_xpath('//*[@class="even"]')
                        single_data = []
                        if odd_rows:
                            for odd_row in odd_rows:
                                output_json = {}
                                output_json['canalOSucursa'] = 'OPERACIONES CENTRALES'
                                data = odd_row.find_elements_by_xpath('./td')
                                output_json['fecha'] = f_operac = data[0].text
                                output_json['descripcion'] = refercia = data[1].text
                                output_json['saldo'] = itf = data[3].text
                                output_json['documento'] = num_mvto = data[4].text
                                importe = float(data[2].text.strip())
                                if importe > 0:
                                    output_json['cargos'] = ''
                                    output_json['abonos'] = '$ ' + str(importe)
                                else:
                                    output_json['cargos'] = '$ ' + str(importe)
                                    output_json['abonos'] = ''
                                all_data.append(output_json)
                        if even_rows:
                            for even_row in even_rows:
                                output_json = {}
                                output_json['canalOSucursa'] = 'OPERACIONES CENTRALES'
                                data = even_row.find_elements_by_xpath('./td')
                                output_json['fecha'] = f_operac = data[0].text
                                output_json['descripcion'] = refercia = data[1].text
                                output_json['saldo'] = itf = data[3].text
                                output_json['documento'] = num_mvto = data[4].text
                                importe = float(data[2].text.strip())
                                if importe > 0:
                                    output_json['cargos'] = ''
                                    output_json['abonos'] = '$ ' + str(importe)
                                else:
                                    output_json['cargos'] = '$ ' + str(importe)
                                    output_json['abonos'] = ''
                                all_data.append(output_json)

                    i = i + 1
            except Exception as e:
                print(str(e))
            try:
                driver.switch_to.default_content()
                driver.find_element_by_xpath('//*[@id="botonDesconectar"]').click()
                sleep(3)
                print('SuccessFully Log Out')
            except Exception as e:
                print(str(e))
            finalres = {"status": {"value": 0, "description": "OK", "TimeElapsed": str(int(time() - timestart))},
                        "request": request_data,
                        "items": all_data}
            blob_client.upload_blob(str(finalres), encoding='utf-8',
                                    content_settings=ContentSettings(content_type='application/json'))
            # print(output_res)
        except Exception as e:
            print(str(e))
            # print("There is no data right now.")
        driver.quit()
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(exc_type, exc_tb.tb_lineno)
        screen = driver.get_screenshot_as_base64()
        finalres = {"status": {"value": 1, "description": f"Error {exc_type} in line {exc_tb.tb_lineno}",
                               "screenshot": str(screen),
                               "TimeElapsed": str(int(time() - timestart))}, "request": request_data,
                    "items": []}
        blob_client.upload_blob(str(finalres), encoding='utf-8',
                                content_settings=ContentSettings(content_type='application/json'))
        driver.close()
    if request.headers.get("EndpointURL") is not None and request.headers.get("EndpointURL") != "":
        lastheaders = {"SpiderJobId": filename}
        post(url=request.headers.get("EndpointURL"), headers=lastheaders)


if __name__ == "__main__":
    handler = RotatingFileHandler('./Logs.log', maxBytes=10000, backupCount=1)
    handler.setLevel(logging.INFO)
    app.run()
