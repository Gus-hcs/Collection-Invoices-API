import pyodbc
import requests
import xml.etree.ElementTree as ET

# Configurações de conexão ao banco de dados
server = '###'
database = '###'
username = '###'
password = '###'

# Conectando ao banco de dados
connection = pyodbc.connect(f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}')
cursor = connection.cursor()

# Namespace para encontrar os elementos XML
namespace = {'nfe': 'http://www.portalfiscal.inf.br/nfe'}

def get_xml_data(xml_url):
    try:
        xml_response = requests.get(xml_url)

        if xml_response.status_code == 200:
            return xml_response.text
        else:
            print(f"Erro na solicitação HTTP do link XML: Código {xml_response.status_code}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"Erro ao conectar-se à API: {str(e)}")
        return None

def process_xml_data(xml_data):
    try:
        root = ET.fromstring(xml_data)
        infnfe_elements = root.findall('.//nfe:infNFe', namespaces=namespace)
        for infnfe_element in infnfe_elements:
                
                ide_element = infnfe_element.find('.//nfe:ide', namespaces=namespace)
                nfe_id = str(infnfe_element.get('Id'))
                cUF_ide = str(ide_element.find('.//nfe:cUF', namespaces=namespace).text) if ide_element.find('.//nfe:cUF', namespaces=namespace) is not None else '0'
                cNF_ide = str(ide_element.find('.//nfe:cNF', namespaces=namespace).text) if ide_element.find('.//nfe:cNF', namespaces=namespace) is not None else '0'
                natOp_ide = str(ide_element.find('.//nfe:natOp', namespaces=namespace).text) if ide_element.find('.//nfe:natOp', namespaces=namespace) is not None else '0'
                mod_ide = str(ide_element.find('.//nfe:mod', namespaces=namespace).text) if ide_element.find('.//nfe:mod', namespaces=namespace) is not None else '0'
                serie_ide = str(ide_element.find('.//nfe:serie', namespaces=namespace).text) if ide_element.find('.//nfe:serie', namespaces=namespace) is not None else '0'
                nNF_ide = str(ide_element.find('.//nfe:nNF', namespaces=namespace).text) if ide_element.find('.//nfe:nNF', namespaces=namespace) is not None else '0'
                dhEmi_ide = str(ide_element.find('.//nfe:dhEmi', namespaces=namespace).text) if ide_element.find('.//nfe:dhEmi', namespaces=namespace) is not None else '0'
                dhSaiEnt_ide = str(ide_element.find('.//nfe:dhSaiEnt', namespaces=namespace).text) if ide_element.find('.//nfe:dhSaiEnt', namespaces=namespace) is not None else '0'
                tpNF_ide = str(ide_element.find('.//nfe:tpNF', namespaces=namespace).text) if ide_element.find('.//nfe:tpNF', namespaces=namespace) is not None else '0'
                idDest_ide = str(ide_element.find('.//nfe:idDest', namespaces=namespace).text) if ide_element.find('.//nfe:idDest', namespaces=namespace) is not None else '0'
                cMunFG_ide = str(ide_element.find('.//nfe:cMunFG', namespaces=namespace).text) if ide_element.find('.//nfe:cMunFG', namespaces=namespace) is not None else '0'
                tpImp_ide = str(ide_element.find('.//nfe:tpImp', namespaces=namespace).text) if ide_element.find('.//nfe:tpImp', namespaces=namespace) is not None else '0'
                tpEmis_ide = str(ide_element.find('.//nfe:tpEmis', namespaces=namespace).text) if ide_element.find('.//nfe:tpEmis', namespaces=namespace) is not None else '0'
                cDV_ide = str(ide_element.find('.//nfe:cDV', namespaces=namespace).text) if ide_element.find('.//nfe:cDV', namespaces=namespace) is not None else '0'
                tpAmb_ide = str(ide_element.find('.//nfe:tpAmb', namespaces=namespace).text) if ide_element.find('.//nfe:tpAmb', namespaces=namespace) is not None else '0'
                finNFe_ide = str(ide_element.find('.//nfe:finNFe', namespaces=namespace).text) if ide_element.find('.//nfe:finNFe', namespaces=namespace) is not None else '0'
                indFinal_ide = str(ide_element.find('.//nfe:indFinal', namespaces=namespace).text) if ide_element.find('.//nfe:indFinal', namespaces=namespace) is not None else '0'
                indPres_ide = str(ide_element.find('.//nfe:indPres', namespaces=namespace).text) if ide_element.find('.//nfe:indPres', namespaces=namespace) is not None else '0'
                indIntermed_ide = str(ide_element.find('.//nfe:indIntermed', namespaces=namespace).text) if ide_element.find('.//nfe:indIntermed', namespaces=namespace) is not None else '0'
                procEmi_ide = str(ide_element.find('.//nfe:procEmi', namespaces=namespace).text) if ide_element.find('.//nfe:procEmi', namespaces=namespace) is not None else '0'
                verProc_ide = str(ide_element.find('.//nfe:verProc', namespaces=namespace).text) if ide_element.find('.//nfe:verProc', namespaces=namespace) is not None else '0'

                emit_element = infnfe_element.find('.//nfe:emit', namespaces=namespace)
                CNPJ_emit = str(emit_element.find('.//nfe:CNPJ', namespaces=namespace).text) if emit_element.find('.//nfe:CNPJ', namespaces=namespace) is not None else '0'
                xNome_emit = str(emit_element.find('.//nfe:xNome', namespaces=namespace).text) if emit_element.find('.//nfe:xNome', namespaces=namespace) is not None else '0'
                xFant_emit = str(emit_element.find('.//nfe:xFant', namespaces=namespace).text) if emit_element.find('.//nfe:xFant', namespaces=namespace) is not None else '0'
                
                enderEmit_element = emit_element.find('.//nfe:enderEmit', namespaces=namespace)
                xLgr_enderEmit = str(enderEmit_element.find('.//nfe:xLgr', namespaces=namespace).text) if enderEmit_element.find('.//nfe:xLgr', namespaces=namespace) is not None else '0'
                nro_enderEmit = str(enderEmit_element.find('.//nfe:nro', namespaces=namespace).text) if enderEmit_element.find('.//nfe:nro', namespaces=namespace) is not None else '0'
                xCpl_enderEmit = str(enderEmit_element.find('.//nfe:xCpl', namespaces=namespace).text) if enderEmit_element.find('.//nfe:xCpl', namespaces=namespace) is not None else '0'
                xBairro_enderEmit = str(enderEmit_element.find('.//nfe:xBairro', namespaces=namespace).text) if enderEmit_element.find('.//nfe:xBairro', namespaces=namespace) is not None else '0'
                cMun_enderEmit = str(enderEmit_element.find('.//nfe:cMun', namespaces=namespace).text) if enderEmit_element.find('.//nfe:cMun', namespaces=namespace) is not None else '0'
                xMun_enderEmit = str(enderEmit_element.find('.//nfe:xMun', namespaces=namespace).text) if enderEmit_element.find('.//nfe:xMun', namespaces=namespace) is not None else '0'
                UF_enderEmit = str(enderEmit_element.find('.//nfe:UF', namespaces=namespace).text) if enderEmit_element.find('.//nfe:UF', namespaces=namespace) is not None else '0'
                CEP_enderEmit = str(enderEmit_element.find('.//nfe:CEP', namespaces=namespace).text) if enderEmit_element.find('.//nfe:CEP', namespaces=namespace) is not None else '0'
                cPais_enderEmit = str(enderEmit_element.find('.//nfe:cPais', namespaces=namespace).text) if enderEmit_element.find('.//nfe:cPais', namespaces=namespace) is not None else '0'
                xPais_enderEmit = str(enderEmit_element.find('.//nfe:xPais', namespaces=namespace).text) if enderEmit_element.find('.//nfe:xPais', namespaces=namespace) is not None else '0'
                fone_enderEmit = str(enderEmit_element.find('.//nfe:fone', namespaces=namespace).text) if enderEmit_element.find('.//nfe:fone', namespaces=namespace) is not None else '0'


                IE_emit = str(emit_element.find('.//nfe:IE', namespaces=namespace).text) if emit_element.find('.//nfe:IE', namespaces=namespace) is not None else '0'
                IM_emit = str(emit_element.find('.//nfe:IM', namespaces=namespace).text) if emit_element.find('.//nfe:IM', namespaces=namespace) is not None else '0'
                CNAE_emit = str(emit_element.find('.//nfe:CNAE', namespaces=namespace).text) if emit_element.find('.//nfe:CNAE', namespaces=namespace) is not None else '0'
                CRT_emit = str(emit_element.find('.//nfe:CRT', namespaces=namespace).text) if emit_element.find('.//nfe:CRT', namespaces=namespace) is not None else '0'

                dest_element = infnfe_element.find('.//nfe:dest', namespaces=namespace)
                CNPJ_dest = str(dest_element.find('.//nfe:CNPJ', namespaces=namespace).text) if dest_element.find('.//nfe:CNPJ', namespaces=namespace) is not None else '0'
                CPF_dest = str(dest_element.find('.//nfe:CPF', namespaces=namespace).text) if dest_element.find('.//nfe:CPF', namespaces=namespace) is not None else '0'
                xNome_dest = str(dest_element.find('.//nfe:xNome', namespaces=namespace).text) if dest_element.find('.//nfe:xNome', namespaces=namespace) is not None else '0'
                indIEDest_dest = str(dest_element.find('.//nfe:indIEDest', namespaces=namespace).text) if dest_element.find('.//nfe:indIEDest', namespaces=namespace) is not None else '0'

                enderDest_element = dest_element.find('.//nfe:enderDest', namespaces=namespace)
                xLgr_enderDest = str(enderDest_element.find('.//nfe:xLgr', namespaces=namespace).text) if enderDest_element.find('.//nfe:xLgr', namespaces=namespace) is not None else '0'
                nro_enderDest = str(enderDest_element.find('.//nfe:nro', namespaces=namespace).text) if enderDest_element.find('.//nfe:nro', namespaces=namespace) is not None else '0'
                xBairro_enderDest = str(enderDest_element.find('.//nfe:xBairro', namespaces=namespace).text) if enderDest_element.find('.//nfe:xBairro', namespaces=namespace) is not None else '0'
                cMun_enderDest = str(enderDest_element.find('.//nfe:cMun', namespaces=namespace).text) if enderDest_element.find('.//nfe:cMun', namespaces=namespace) is not None else '0'
                xMun_enderDest = str(enderDest_element.find('.//nfe:xMun', namespaces=namespace).text) if enderDest_element.find('.//nfe:xMun', namespaces=namespace) is not None else '0'
                UF_enderDest = str(enderDest_element.find('.//nfe:UF', namespaces=namespace).text) if enderDest_element.find('.//nfe:UF', namespaces=namespace) is not None else '0'
                CEP_enderDest = str(enderDest_element.find('.//nfe:CEP', namespaces=namespace).text) if enderDest_element.find('.//nfe:CEP', namespaces=namespace) is not None else '0'
                cPais_enderDest = str(enderDest_element.find('.//nfe:cPais', namespaces=namespace).text) if enderDest_element.find('.//nfe:cPais', namespaces=namespace) is not None else '0'
                xPais_enderDest = str(enderDest_element.find('.//nfe:xPais', namespaces=namespace).text) if enderDest_element.find('.//nfe:xPais', namespaces=namespace) is not None else '0'

                det_elements = infnfe_element.findall('.//nfe:det', namespaces=namespace)  

                total_element = infnfe_element.find('.//nfe:total', namespaces=namespace)
                ICMSTot_element = total_element.find('.//nfe:ICMSTot', namespaces=namespace) if total_element.find('.//nfe:ICMSTot', namespaces=namespace) is not None else '0'
                vBC_ICMSTot = str(ICMSTot_element.find('.//nfe:vBC', namespaces=namespace).text) if ICMSTot_element.find('.//nfe:vBC', namespaces=namespace) is not None else '0'
                vICMS_ICMSTot = str(ICMSTot_element.find('.//nfe:vICMS', namespaces=namespace).text) if ICMSTot_element.find('.//nfe:vICMS', namespaces=namespace) is not None else '0'
                vICMSDeson_ICMSTot = str(ICMSTot_element.find('.//nfe:vICMSDeson', namespaces=namespace).text) if ICMSTot_element.find('.//nfe:vICMSDeson', namespaces=namespace) is not None else '0'
                vFCP_ICMSTot = str(ICMSTot_element.find('.//nfe:vFCP', namespaces=namespace).text) if ICMSTot_element.find('.//nfe:vFCP', namespaces=namespace) is not None else '0'
                vBCST_ICMSTot = str(ICMSTot_element.find('.//nfe:vBCST', namespaces=namespace).text) if ICMSTot_element.find('.//nfe:vBCST', namespaces=namespace) is not None else '0'
                vST_ICMSTot = str(ICMSTot_element.find('.//nfe:vST', namespaces=namespace).text) if ICMSTot_element.find('.//nfe:vST', namespaces=namespace) is not None else '0'
                vFCPST_ICMSTot = str(ICMSTot_element.find('.//nfe:vFCPST', namespaces=namespace).text) if ICMSTot_element.find('.//nfe:vFCPST', namespaces=namespace) is not None else '0'
                vFCPSTRet_ICMSTot = str(ICMSTot_element.find('.//nfe:vFCPSTRet', namespaces=namespace).text) if ICMSTot_element.find('.//nfe:vFCPSTRet', namespaces=namespace) is not None else '0'
                vProd_Total_ICMSTot = str(ICMSTot_element.find('.//nfe:vProd', namespaces=namespace).text) if ICMSTot_element.find('.//nfe:vProd', namespaces=namespace) is not None else '0'
                vFrete_ICMSTot = str(ICMSTot_element.find('.//nfe:vFrete', namespaces=namespace).text) if ICMSTot_element.find('.//nfe:vFrete', namespaces=namespace) is not None else '0'
                vSeg_ICMSTot = str(ICMSTot_element.find('.//nfe:vSeg', namespaces=namespace).text) if ICMSTot_element.find('.//nfe:vSeg', namespaces=namespace) is not None else '0'
                vDesc_ICMSTot = str(ICMSTot_element.find('.//nfe:vDesc', namespaces=namespace).text) if ICMSTot_element.find('.//nfe:vDesc', namespaces=namespace) is not None else '0'
                vII_ICMSTot = str(ICMSTot_element.find('.//nfe:vII', namespaces=namespace).text) if ICMSTot_element.find('.//nfe:vII', namespaces=namespace) is not None else '0'
                vIPI_ICMSTot = str(ICMSTot_element.find('.//nfe:vIPI', namespaces=namespace).text) if ICMSTot_element.find('.//nfe:vIPI', namespaces=namespace) is not None else '0'
                vIPIDevol_ICMSTot = str(ICMSTot_element.find('.//nfe:vIPIDevol', namespaces=namespace).text) if ICMSTot_element.find('.//nfe:vIPIDevol', namespaces=namespace) is not None else '0'
                vPIS_ICMSTot = str(ICMSTot_element.find('.//nfe:vPIS', namespaces=namespace).text) if ICMSTot_element.find('.//nfe:vPIS', namespaces=namespace) is not None else '0'
                vCOFINS_ICMSTot = str(ICMSTot_element.find('.//nfe:vCOFINS', namespaces=namespace).text) if ICMSTot_element.find('.//nfe:vCOFINS', namespaces=namespace) is not None else '0'
                vOutro_ICMSTot = str(ICMSTot_element.find('.//nfe:vOutro', namespaces=namespace).text) if ICMSTot_element.find('.//nfe:vOutro', namespaces=namespace) is not None else '0'
                vNF_ICMSTot = str(ICMSTot_element.find('.//nfe:vNF', namespaces=namespace).text) if ICMSTot_element.find('.//nfe:vNF', namespaces=namespace) is not None else '0'
                vTotTrib_ICMSTot = str(ICMSTot_element.find('.//nfe:vTotTrib', namespaces=namespace).text) if ICMSTot_element.find('.//nfe:vTotTrib', namespaces=namespace) is not None else '0'

                ide_element = infnfe_element.find('.//nfe:pag', namespaces=namespace)
                if ide_element is not None:
                    tPag_ide = str(ide_element.find('.//nfe:tPag', namespaces=namespace).text) if ide_element.find('.//nfe:tPag', namespaces=namespace) is not None else '0'

                            
                for det_element in det_elements:
                    n_item = str(det_element.get('nItem'))

                    prod_element = det_element.find('.//nfe:prod', namespaces=namespace)
                    cProd_prod = str(prod_element.find('.//nfe:cProd', namespaces=namespace).text) if prod_element.find('.//nfe:cProd', namespaces=namespace) is not None else '0'
                    cEAN_prod = str(prod_element.find('.//nfe:cEAN', namespaces=namespace).text) if prod_element.find('.//nfe:cEAN', namespaces=namespace) is not None else '0'
                    xProd_prod = str(prod_element.find('.//nfe:xProd', namespaces=namespace).text) if prod_element.find('.//nfe:xProd', namespaces=namespace) is not None else '0'
                    NCM_prod = str(prod_element.find('.//nfe:NCM', namespaces=namespace).text) if prod_element.find('.//nfe:NCM', namespaces=namespace) is not None else '0'
                    CFOP_prod = str(prod_element.find('.//nfe:CFOP', namespaces=namespace).text) if prod_element.find('.//nfe:CFOP', namespaces=namespace) is not None else '0'
                    uCom_prod = str(prod_element.find('.//nfe:uCom', namespaces=namespace).text) if prod_element.find('.//nfe:uCom', namespaces=namespace) is not None else '0'
                    qCom_prod = str(prod_element.find('.//nfe:qCom', namespaces=namespace).text) if prod_element.find('.//nfe:qCom', namespaces=namespace) is not None else '0'
                    vUnCom_prod = str(prod_element.find('.//nfe:vUnCom', namespaces=namespace).text) if prod_element.find('.//nfe:vUnCom', namespaces=namespace) is not None else '0'
                    vProd_prod = str(prod_element.find('.//nfe:vProd', namespaces=namespace).text) if prod_element.find('.//nfe:vProd', namespaces=namespace) is not None else '0'
                    cEANTrib_prod = str(prod_element.find('.//nfe:cEANTrib', namespaces=namespace).text) if prod_element.find('.//nfe:cEANTrib', namespaces=namespace) is not None else '0'
                    uTrib_prod = str(prod_element.find('.//nfe:uTrib', namespaces=namespace).text) if prod_element.find('.//nfe:uTrib', namespaces=namespace) is not None else '0'
                    qTrib_prod = str(prod_element.find('.//nfe:qTrib', namespaces=namespace).text) if prod_element.find('.//nfe:qTrib', namespaces=namespace) is not None else '0'
                    vUnTrib_prod = str(prod_element.find('.//nfe:vUnTrib', namespaces=namespace).text) if prod_element.find('.//nfe:vUnTrib', namespaces=namespace) is not None else '0'
                    indTot_prod = str(prod_element.find('.//nfe:indTot', namespaces=namespace).text) if prod_element.find('.//nfe:indTot', namespaces=namespace) is not None else '0'


                    imposto_element = det_element.find('.//nfe:imposto', namespaces=namespace)
                    vTotTrib_imposto = str(imposto_element.find('.//nfe:vTotTrib', namespaces=namespace).text) if imposto_element.find('.//nfe:vTotTrib', namespaces=namespace) is not None else '0'

                    icms_element = imposto_element.find('.//nfe:ICMS', namespaces=namespace)
                    icms10_element = icms_element.find('.//nfe:ICMS10', namespaces=namespace) if icms_element is not None else None
                    icms00_element = icms_element.find('.//nfe:ICMS00', namespaces=namespace) if icms_element is not None else None

                    try:
                        pMVAST = str(icms10_element.find('.//nfe:pMVAST', namespaces=namespace).text)
                    except AttributeError:
                        pMVAST = '0'

                    try:
                        orig_icms00 = str(icms00_element.find('.//nfe:orig', namespaces=namespace).text)
                    except AttributeError:
                        orig_icms00 = '0'

                    try:
                        CST_icms00 = str(icms00_element.find('.//nfe:CST', namespaces=namespace).text)
                    except AttributeError:
                        CST_icms00 = '0'

                    try:
                        modBC_icms00 = str(icms00_element.find('.//nfe:modBC', namespaces=namespace).text)
                    except AttributeError:
                        modBC_icms00 = '0'

                    try:
                        vBC_icms00 = str(icms00_element.find('.//nfe:vBC', namespaces=namespace).text)
                    except AttributeError:
                        vBC_icms00 = '0'

                    try:
                        pICMS_icms00 = str(icms00_element.find('.//nfe:pICMS', namespaces=namespace).text)
                    except AttributeError:
                        pICMS_icms00 = '0'

                    try:
                        vICMS_icms00 = str(icms00_element.find('.//nfe:vICMS', namespaces=namespace).text)
                    except AttributeError:
                        vICMS_icms00 = '0'

                    try:
                        ipi_element = imposto_element.find('.//nfe:IPI', namespaces=namespace)
                        cEnq_ipi = str(ipi_element.find('.//nfe:cEnq', namespaces=namespace).text) if ipi_element is not None and ipi_element.find('.//nfe:cEnq', namespaces=namespace) is not None else '0'
                        ipint_element = imposto_element.find('.//nfe:IPINT', namespaces=namespace)
                        CST_ipi_imposto = str(imposto_element.find('.//nfe:CST', namespaces=namespace).text) if ipi_element is not None and imposto_element.find('.//nfe:CST', namespaces=namespace) is not None else '0'
                    except (AttributeError, TypeError) as e:
                        print(f"Erro ao obter informações do IPI: {e}")
                        cEnq_ipi = '0'
                        CST_ipi_imposto = '0'

                    try:
                        pis_element = imposto_element.find('.//nfe:PIS', namespaces=namespace)
                        pisaliq_element = pis_element.find('.//nfe:PISAliq', namespaces=namespace)
                        CST_PIS = str(pisaliq_element.find('.//nfe:CST', namespaces=namespace).text) if pisaliq_element is not None and pisaliq_element.find('.//nfe:CST', namespaces=namespace) is not None else '0'
                        vBC_PIS = str(pisaliq_element.find('.//nfe:vBC', namespaces=namespace).text) if pisaliq_element is not None and pisaliq_element.find('.//nfe:vBC', namespaces=namespace) is not None else '0'
                        pPIS_PIS = str(pisaliq_element.find('.//nfe:pPIS', namespaces=namespace).text) if pisaliq_element is not None and pisaliq_element.find('.//nfe:pPIS', namespaces=namespace) is not None else '0'
                        vPIS_PIS = str(pisaliq_element.find('.//nfe:vPIS', namespaces=namespace).text) if pisaliq_element is not None and pisaliq_element.find('.//nfe:vPIS', namespaces=namespace) is not None else '0'
                    except (AttributeError, TypeError) as e:
                        print(f"Erro ao obter informações do PIS: {e}")
                        CST_PIS = '0'
                        vBC_PIS = '0'
                        pPIS_PIS = '0'
                        vPIS_PIS = '0'

                    try:
                        cofins_element = imposto_element.find('.//nfe:COFINS', namespaces=namespace)
                        cofinsaliq_element = cofins_element.find('.//nfe:COFINSAliq', namespaces=namespace)
                        CST_COFINS_cofinsaliq = str(cofinsaliq_element.find('.//nfe:CST', namespaces=namespace).text) if cofinsaliq_element is not None and cofinsaliq_element.find('.//nfe:CST', namespaces=namespace) is not None else '0'
                        vBC_COFINS_cofinsaliq = str(cofinsaliq_element.find('.//nfe:vBC', namespaces=namespace).text) if cofinsaliq_element is not None and cofinsaliq_element.find('.//nfe:vBC', namespaces=namespace) is not None else '0'
                        pCOFINS_cofinsaliq = str(cofinsaliq_element.find('.//nfe:pCOFINS', namespaces=namespace).text) if cofinsaliq_element is not None and cofinsaliq_element.find('.//nfe:pCOFINS', namespaces=namespace) is not None else '0'
                        vCOFINS_cofinsaliq = str(cofinsaliq_element.find('.//nfe:vCOFINS', namespaces=namespace).text) if cofinsaliq_element is not None and cofinsaliq_element.find('.//nfe:vCOFINS', namespaces=namespace) is not None else '0'
                    except (AttributeError, TypeError) as e:
                        print(f"Erro ao obter informações do COFINS: {e}")
                        CST_COFINS_cofinsaliq = '0'
                        vBC_COFINS_cofinsaliq = '0'
                        pCOFINS_cofinsaliq = '0'
                        vCOFINS_cofinsaliq = '0'

                    cursor.execute(
                        """
                        INSERT INTO bling_notas_saida 
                        (
                            nfe_id, cUF_ide, cNF_ide, natOp_ide, mod_ide, serie_ide, nNF_ide, dhEmi_ide, dhSaiEnt_ide, tpNF_ide, idDest_ide, cMunFG_ide, tpImp_ide, tpEmis_ide, cDV_ide, tpAmb_ide, 
                            finNFe_ide, indFinal_ide, indPres_ide, indIntermed_ide, procEmi_ide, verProc_ide, CNPJ_emit, xNome_emit, xFant_emit, xLgr_enderEmit, nro_enderEmit, xCpl_enderEmit, 
                            xBairro_enderEmit, cMun_enderEmit, xMun_enderEmit, UF_enderEmit, CEP_enderEmit, cPais_enderEmit, xPais_enderEmit, fone_enderEmit, IE_emit, IM_emit, CNAE_emit, CRT_emit, 
                            CNPJ_dest, CPF_dest, xNome_dest, indIEDest_dest, xLgr_enderDest, nro_enderDest, xBairro_enderDest, cMun_enderDest, xMun_enderDest, UF_enderDest, CEP_enderDest, 
                            cPais_enderDest, xPais_enderDest, vBC_ICMSTot, vICMS_ICMSTot, vICMSDeson_ICMSTot, vFCP_ICMSTot, vBCST_ICMSTot, vST_ICMSTot, vFCPST_ICMSTot, 
                            vFCPSTRet_ICMSTot, vProd_Total_ICMSTot, vFrete_ICMSTot, vSeg_ICMSTot, vDesc_ICMSTot, vII_ICMSTot, vIPI_ICMSTot, vIPIDevol_ICMSTot, vPIS_ICMSTot, vCOFINS_ICMSTot, 
                            vOutro_ICMSTot, vNF_ICMSTot, vTotTrib_ICMSTot, cProd_prod, cEAN_prod, xProd_prod, NCM_prod, CFOP_prod, uCom_prod, qCom_prod, vUnCom_prod, vProd_prod, cEANTrib_prod, 
                            uTrib_prod, qTrib_prod, vUnTrib_prod, indTot_prod, vTotTrib_imposto, orig_icms00, CST_icms00, modBC_icms00, vBC_icms00, pICMS_icms00, vICMS_icms00, cEnq_ipi, 
                            CST_ipi_imposto, CST_PIS, vBC_PIS, pPIS_PIS, vPIS_PIS, CST_COFINS_cofinsaliq, vBC_COFINS_cofinsaliq, pCOFINS_cofinsaliq, vCOFINS_cofinsaliq,pMVAST
                        ) 
                        VALUES 
                        (
                            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
                            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
                            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
                            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
                            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                        )
                        """,
                        (
                            nfe_id, cUF_ide, cNF_ide, natOp_ide, mod_ide, serie_ide, nNF_ide, dhEmi_ide, dhSaiEnt_ide, tpNF_ide, idDest_ide, cMunFG_ide, tpImp_ide, tpEmis_ide, cDV_ide, tpAmb_ide, 
                            finNFe_ide, indFinal_ide, indPres_ide, indIntermed_ide, procEmi_ide, verProc_ide, CNPJ_emit, xNome_emit, xFant_emit, xLgr_enderEmit, nro_enderEmit, xCpl_enderEmit, 
                            xBairro_enderEmit, cMun_enderEmit, xMun_enderEmit, UF_enderEmit, CEP_enderEmit, cPais_enderEmit, xPais_enderEmit, fone_enderEmit, IE_emit, IM_emit, CNAE_emit, CRT_emit, 
                            CNPJ_dest, CPF_dest, xNome_dest, indIEDest_dest, xLgr_enderDest, nro_enderDest, xBairro_enderDest, cMun_enderDest, xMun_enderDest, UF_enderDest, CEP_enderDest, 
                            cPais_enderDest, xPais_enderDest, vBC_ICMSTot, vICMS_ICMSTot, vICMSDeson_ICMSTot, vFCP_ICMSTot, vBCST_ICMSTot, vST_ICMSTot, vFCPST_ICMSTot, 
                            vFCPSTRet_ICMSTot, vProd_Total_ICMSTot, vFrete_ICMSTot, vSeg_ICMSTot, vDesc_ICMSTot, vII_ICMSTot, vIPI_ICMSTot, vIPIDevol_ICMSTot, vPIS_ICMSTot, vCOFINS_ICMSTot, 
                            vOutro_ICMSTot, vNF_ICMSTot, vTotTrib_ICMSTot, cProd_prod, cEAN_prod, xProd_prod, NCM_prod, CFOP_prod, uCom_prod, qCom_prod, vUnCom_prod, vProd_prod, cEANTrib_prod, 
                            uTrib_prod, qTrib_prod, vUnTrib_prod, indTot_prod, vTotTrib_imposto, orig_icms00, CST_icms00, modBC_icms00, vBC_icms00, pICMS_icms00, vICMS_icms00, cEnq_ipi, 
                            CST_ipi_imposto, CST_PIS, vBC_PIS, pPIS_PIS, vPIS_PIS, CST_COFINS_cofinsaliq, vBC_COFINS_cofinsaliq, pCOFINS_cofinsaliq, vCOFINS_cofinsaliq,pMVAST
                        )
                    )
                    connection.commit()
    except Exception as e:
        print(f"Erro ao inserir dados no banco de dados: {str(e)}")
    pass

def fetch_and_process_xmls():
    cursor.execute("SELECT xml FROM Validação_XML_Nota")
    rows = cursor.fetchall()
    for row in rows:
        xml_url = row.xml
        xml_data = get_xml_data(xml_url)
        if xml_data:
            process_xml_data(xml_data)

fetch_and_process_xmls()
