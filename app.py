import os
import xmltodict
from concurrent.futures import ThreadPoolExecutor
import time
import pyodbc

# Define la función que procesará un archivo XML y enviará los datos a SQL Server.
def procesar_archivo(file_path):
    try:
        #file_path = r'C:/Users/grv/Documents/proyectos/udem/conciliacion/xml/sat/marzo/UMO780601S4A_EB5910B1-93BD-4BE4-93AE-2D4E955A895D.xml' # 2 nomina
        #file_path = r'C:/Users/grv/Documents/proyectos/udem/conciliacion/xml/sat/enero/UMO780601S4A_00AC9F88-8F94-415D-AF6E-471FEAD99B43.xml' # 1 nomina
        # Abre y parsea el archivo XML
        with open(file_path, encoding='utf-8') as xml_file:
            data_dict = xmltodict.parse(xml_file.read())

        # Extrae y procesa los datos necesarios del archivo XML
        data = extract_and_process_data(root=data_dict)
        
        # Configura la conexión a SQL Server
        connection_string = "Driver={ODBC Driver 11 for SQL Server};Server=172.16.200.142;Database=CONCILIACION;UID=Usr_conciliacion;PWD=C0nc1li4Tst$"
        connection = pyodbc.connect(connection_string)

        # Inserta los datos en la base de datos
        cursor = connection.cursor()

        # Validamos que el UUID no exista ya
        sql = 'SELECT * FROM cfdi WHERE UUID = ?'
        cursor.execute(sql, (data['cfdi'].get('UUID')))
        result = cursor.fetchall()
        if result:
            print(f"Error al procesar {file_path}: UUID duplicado.")
            cursor.close()
            connection.close()
            return

        # Guardamos datos en una tupla
        tuple = (data['cfdi'].get('UUID'), data['cfdi'].get('serie'), data['cfdi'].get('folio'), data['cfdi'].get('fecha'), data['cfdi'].get('formaPago'), data['cfdi'].get('noCertificado'), 
                 data['cfdi'].get('certificado'), data['cfdi'].get('subtotal'), data['cfdi'].get('descuento'), data['cfdi'].get('tipoCambio'), data['cfdi'].get('moneda'), data['cfdi'].get('total'), 
                 data['cfdi'].get('tipoComprobante'), data['cfdi'].get('metodoPago'), data['cfdi'].get('lugarExpedicion'), data['cfdi'].get('emisorRFC'), data['cfdi'].get('emisorNombre'), 
                 data['cfdi'].get('emisorRegimenFiscal'), data['cfdi'].get('receptorRFC'), data['cfdi'].get('receptorNombre'), data['cfdi'].get('receptorUsoCFDI'), data['cfdi'].get('conceptoClaveProdServ'), 
                 data['cfdi'].get('conceptoCantidad'), data['cfdi'].get('conceptoClaveUnidad'), data['cfdi'].get('conceptoDescripcion'), data['cfdi'].get('conceptoValorUnitario'), data['cfdi'].get('conceptoImporte'), data['cfdi'].get('conceptoDescuento'), data['cfdi'].get('timbreSelloCFD'), data['cfdi'].get('timbreNoCertificadoSAT'), data['cfdi'].get('timbreSelloSAT'))

        # Creamos query para Insert
        sql = """INSERT INTO cfdi (
                    uuid, serie, folio, fecha, formaPago, noCertificado, certificado, subtotal, descuento, tipoCambio, moneda, total, tipoComprobante, metodoPago, 
                    lugarExpedicion, emisorRFC, emisorNombre, emisorRegimenFiscal, receptorRFC, receptorNombre, receptorUsoCFDI, conceptoClaveProdServ, 
                    conceptoCantidad, conceptoClaveUnidad, conceptoDescripcion, conceptoValorUnitario, conceptoImporte, conceptoDescuento, timbreSelloCFD, 
                    timbreNoCertificadoSAT, timbreSelloSAT)  VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""

        # Insertamos y traemos ID insertado
        cursor.execute(sql, tuple)
        cursor.execute("SELECT @@IDENTITY AS ID;")
        idCfdi = cursor.fetchone()[0]

        # Inserta nomina
        for nomina in data['nomina']:
            tuple = (idCfdi,  nomina.get('posicion'), nomina.get('tipoNomina'), nomina.get('fechaPago'), nomina.get('fechaInicialPago'), nomina.get('fechaFinalPago'), nomina.get('numDiasPagados'), 
                     nomina.get('totalPercepciones'), nomina.get('totalDeducciones'), nomina.get('totalOtrosPagos'), nomina.get('emisor_RegistroPatronal'), nomina.get('receptor_curp'), 
                     nomina.get('receptor_numSeguridadSocial'), nomina.get('receptor_fechaInicioRelLaboral'), nomina.get('receptor_antiguedad'), nomina.get('receptor_tipoContrato'), 
                     nomina.get('receptor_tipoJornada'), nomina.get('receptor_tipoRegimen'), nomina.get('receptor_numEmpleado'), nomina.get('receptor_departamento'), nomina.get('receptor_puesto'), 
                     nomina.get('receptor_riesgoPuesto'), nomina.get('receptor_periodicidadPago'), nomina.get('receptor_banco'), nomina.get('receptor_cuentaBancaria'), nomina.get('receptor_salarioBaseCotApor'), 
                     nomina.get('receptor_salarioDiarioIntegrado'), nomina.get('receptor_claveEntFed'))
            
            sql = """INSERT INTO nomina (
                idCfdi, posicion, tipoNomina, fechaPago, fechaInicialPago, fechaFinalPago, numDiasPagados, totalPercepciones, totalDeducciones, totalOtrosPagos, emisor_RegistroPatronal, 
                receptor_curp, receptor_numSeguridadSocial, receptor_fechaInicioRelLaboral, receptor_antiguedad, receptor_tipoContrato, receptor_tipoJornada, receptor_tipoRegimen, 
                receptor_numEmpleado, receptor_departamento, receptor_puesto, receptor_riesgoPuesto, receptor_periodicidadPago, receptor_banco, receptor_cuentaBancaria, 
                receptor_salarioBaseCotApor, receptor_salarioDiarioIntegrado, receptor_claveEntFed
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""

            cursor.execute(sql, tuple)
            cursor.execute("SELECT @@IDENTITY AS ID;")
            idNomina = cursor.fetchone()[0]
            
            # Inserta desgloses
            for desglose in nomina['desgloses']:
                tuple = (idNomina, desglose['posicion'], desglose['categoria'], desglose['subcategoria'], desglose['atributo'], desglose['valor'])
                sql = """INSERT INTO desgloseNomina (
                    idNomina, posicion, categoria, subcategoria, atributo, valor
                ) values(?, ?, ?, ?, ?, ?)"""
                cursor.execute(sql, tuple)
        
        cursor.commit()
        cursor.close()
        connection.close()

        # Indica que el archivo ha sido procesado
        #print(f"Procesado: {file_path}")

    except Exception as e:
        cursor.close()
        connection.close()
        print(f"Error al procesar {file_path}: {e}")

def extract_and_process_data(root):
    # Leer datos generales de XML
    data = {'cfdi': {}, 'nomina': [], 'desglose_nomina': []}
    cfdi_comprobante = root['cfdi:Comprobante']
    
    data['cfdi']['UUID'] = cfdi_comprobante['cfdi:Complemento']['tfd:TimbreFiscalDigital']['@UUID']
    data['cfdi']['serie'] = cfdi_comprobante.get('@Serie')
    data['cfdi']['folio'] = cfdi_comprobante.get('@Folio')
    data['cfdi']['fecha'] = cfdi_comprobante.get('@Fecha')
    data['cfdi']['formaPago'] = cfdi_comprobante.get('@FormaPago')
    data['cfdi']['noCertificado'] = cfdi_comprobante.get('@NoCertificado')
    data['cfdi']['certificado'] = cfdi_comprobante.get('@Certificado')
    data['cfdi']['subtotal'] = cfdi_comprobante.get('@SubTotal')
    data['cfdi']['descuento'] = cfdi_comprobante.get('@Descuento')
    data['cfdi']['tipoCambio'] = cfdi_comprobante.get('@TipoCambio')
    data['cfdi']['moneda'] = cfdi_comprobante.get('@Moneda')
    data['cfdi']['total'] = cfdi_comprobante.get('@Total')
    data['cfdi']['tipoComprobante'] = cfdi_comprobante.get('@TipoDeComprobante')
    data['cfdi']['metodoPago'] = cfdi_comprobante.get('@MetodoPago')
    data['cfdi']['lugarExpedicion'] = cfdi_comprobante.get('@LugarExpedicion')
    data['cfdi']['emisorRFC'] = cfdi_comprobante['cfdi:Emisor'].get('@Rfc')
    data['cfdi']['emisorNombre'] = cfdi_comprobante['cfdi:Emisor'].get('@Nombre')
    data['cfdi']['emisorRegimenFiscal'] = cfdi_comprobante['cfdi:Emisor'].get('@RegimenFiscal')
    data['cfdi']['receptorRFC'] = cfdi_comprobante['cfdi:Receptor'].get('@Rfc')
    data['cfdi']['receptorNombre'] = cfdi_comprobante['cfdi:Receptor'].get('@Nombre')
    data['cfdi']['receptorUsoCFDI'] = cfdi_comprobante['cfdi:Receptor'].get('@UsoCFDI')
    data['cfdi']['conceptoClaveProdServ'] = cfdi_comprobante['cfdi:Conceptos']['cfdi:Concepto'].get('@ClaveProdServ')
    data['cfdi']['conceptoCantidad'] = cfdi_comprobante['cfdi:Conceptos']['cfdi:Concepto'].get('@Cantidad')
    data['cfdi']['conceptoClaveUnidad'] = cfdi_comprobante['cfdi:Conceptos']['cfdi:Concepto'].get('@ClaveUnidad')
    data['cfdi']['conceptoDescripcion'] = cfdi_comprobante['cfdi:Conceptos']['cfdi:Concepto'].get('@Descripcion')
    data['cfdi']['conceptoValorUnitario'] = cfdi_comprobante['cfdi:Conceptos']['cfdi:Concepto'].get('@ValorUnitario')
    data['cfdi']['conceptoImporte'] = cfdi_comprobante['cfdi:Conceptos']['cfdi:Concepto'].get('@Importe')
    data['cfdi']['conceptoDescuento'] = cfdi_comprobante['cfdi:Conceptos']['cfdi:Concepto'].get('@Descuento')
    data['cfdi']['timbreSelloCFD'] = cfdi_comprobante['cfdi:Complemento']['tfd:TimbreFiscalDigital'].get('@SelloCFD')
    data['cfdi']['timbreNoCertificadoSAT'] = cfdi_comprobante['cfdi:Complemento']['tfd:TimbreFiscalDigital'].get('@NoCertificadoSAT')
    data['cfdi']['timbreSelloSAT'] = cfdi_comprobante['cfdi:Complemento']['tfd:TimbreFiscalDigital'].get('@SelloSAT')

    # Validar si existe mas de un nodo nomina
    nominas = cfdi_comprobante['cfdi:Complemento']['nomina12:Nomina']
    if type(nominas) is not list:
        # Si solo existe un nodo lo agregamos a un arreglo
        nominas = [nominas]
    
    # Recorrer arreglo nominas
    for idx, item_nomina in enumerate(nominas):
        # Leer datos generales de nomina
        nomina = {}
        nomina['posicion'] = idx + 1
        nomina['UUID'] = cfdi_comprobante['cfdi:Complemento']['tfd:TimbreFiscalDigital']['@UUID']
        nomina['tipoNomina'] = item_nomina.get('@TipoNomina')
        nomina['fechaPago'] = item_nomina.get('@FechaPago')
        nomina['fechaInicialPago'] = item_nomina.get('@FechaInicialPago')
        nomina['fechaFinalPago'] = item_nomina.get('@FechaFinalPago')
        nomina['numDiasPagados'] = item_nomina.get('@NumDiasPagados')
        nomina['totalPercepciones'] = item_nomina.get('@TotalPercepciones')
        nomina['totalDeducciones'] = item_nomina.get('@TotalDeducciones')
        nomina['totalOtrosPagos'] = item_nomina.get('@TotalOtrosPagos')

        # Validar que exista Emisor
        if 'nomina12:Emisor' in item_nomina.keys():
            nomina['emisor_RegistroPatronal'] = item_nomina['nomina12:Emisor'].get('@RegistroPatronal')
        
        # Validar que exista Receptor
        if 'nomina12:Receptor' in item_nomina.keys():
            nomina['receptor_curp'] = item_nomina['nomina12:Receptor'].get('@Curp')
            nomina['receptor_numSeguridadSocial'] = item_nomina['nomina12:Receptor'].get('@NumSeguridadSocial')
            nomina['receptor_fechaInicioRelLaboral'] = item_nomina['nomina12:Receptor'].get('@FechaInicioRelLaboral')
            nomina['receptor_antiguedad'] = item_nomina['nomina12:Receptor'].get('@Antigüedad')
            nomina['receptor_tipoContrato'] = item_nomina['nomina12:Receptor'].get('@TipoContrato')
            nomina['receptor_tipoJornada'] = item_nomina['nomina12:Receptor'].get('@TipoJornada')
            nomina['receptor_tipoRegimen'] = item_nomina['nomina12:Receptor'].get('@TipoRegimen')
            nomina['receptor_numEmpleado'] = item_nomina['nomina12:Receptor'].get('@NumEmpleado')
            nomina['receptor_departamento'] = item_nomina['nomina12:Receptor'].get('@Departamento')
            nomina['receptor_puesto'] = item_nomina['nomina12:Receptor'].get('@Puesto')
            nomina['receptor_riesgoPuesto'] = item_nomina['nomina12:Receptor'].get('@RiesgoPuesto')
            nomina['receptor_periodicidadPago'] = item_nomina['nomina12:Receptor'].get('@PeriodicidadPago')
            nomina['receptor_banco'] = item_nomina['nomina12:Receptor'].get('@Banco')
            nomina['receptor_cuentaBancaria'] = item_nomina['nomina12:Receptor'].get('@CuentaBancaria')
            nomina['receptor_salarioBaseCotApor'] = item_nomina['nomina12:Receptor'].get('@SalarioBaseCotApor')
            nomina['receptor_salarioDiarioIntegrado'] = item_nomina['nomina12:Receptor'].get('@SalarioDiarioIntegrado')
            nomina['receptor_claveEntFed'] = item_nomina['nomina12:Receptor'].get('@ClaveEntFed')
            nomina['desgloses'] = []

        # Vamos a hacerlo generico para que automaticamente recorra percepciones y deducciones
        categories = ['nomina12:Percepciones', 'nomina12:Deducciones', 'nomina12:OtrosPagos']
        for category_value in categories:
            # Si existe percepcion o deduccion entra
            if category_value in item_nomina.keys():
                category = item_nomina[category_value]
                # Recorremos subnodos
                subnode_keys = [key for key in category.keys() if not key.startswith('@')]
                for key in subnode_keys:
                    category_array = category[key]
                    if type(category_array) is not list:
                        category_array = [category_array]
                    
                    # Extraemos info de cada elemento dentro del arreglo de subnodos
                    for subidx, subcategory in enumerate(category_array):
                        for attribute in [attr for attr in subcategory if attr.startswith('@')]:
                            element = {
                                'posicion': subidx + 1,
                                'categoria': category_value,
                                'subcategoria': key,
                                'atributo': attribute[1:],
                                'valor': subcategory[attribute]
                            }
                            nomina['desgloses'].append(element)

        # Agregamos nomina al arreglo
        data['nomina'].append(nomina)
    return data

def start():
    # Obtiene la ruta del directorio donde se encuentra este script
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # Agrega la carpeta "xml" al directorio raiz
    directorio_raiz = os.path.join(script_dir, "xml")
    print(f'Directorio raiz: {directorio_raiz}')

    # Lista de archivos XML en todas las carpetas de manera recursiva
    archivos_xml = []
    for root, _, files in os.walk(directorio_raiz):
        for file_name in files:
            if file_name.endswith(".xml"):
                archivos_xml.append(os.path.join(root, file_name))
    print(f'Total de archivos: {len(archivos_xml)}')

    # Configura el número de hilos para la concurrencia
    available_cpus = os.cpu_count() - 1
    print(f"CPU disponibles: {available_cpus}")
    num_hilos = available_cpus  # Puedes ajustar este número según tus necesidades

    # Inicia el temporizador
    start_time = time.time()
    formated_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(start_time))
    print(f'Hora de inicio: {formated_time}')

    # Utiliza ThreadPoolExecutor para procesar los archivos en paralelo
    with ThreadPoolExecutor(max_workers=num_hilos) as executor:
        for archivo in archivos_xml:
            executor.submit(procesar_archivo, archivo)

    # Detiene el temporizador
    end_time = time.time()

    # Calcula el tiempo total transcurrido
    tiempo_transcurrido = end_time - start_time
    print(f"Tiempo total de ejecución: {tiempo_transcurrido:.2f} segundos")
