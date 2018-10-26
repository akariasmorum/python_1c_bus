import json
import re
import traceback
from System import DBNull
from System import DateTime


def createNode(dictionary, name, parent=None, text=None, attributes=None):
    if parent == None:
        dictionary['name'] = name
    if text != None:
        dictionary['text'] = text
    if attributes != None:
        dictionary['attrs'] = attributes
    else:
        # текущий узел - корневой
        nodeTraversal(dictionary, parent, name, text, attributes)


def nodeTraversal(current, parent, name, text=None, attrs=None):
    if current['name'] == parent:
        if 'children' not in current:
            current['children'] = []

        # ВНИМАНИЕ!  переписать кастыль
        if text != None and attrs != None:
            current['children'].append({'name': name, 'text': text, 'attrs': attrs})
        elif text != None:
            current['children'].append({'name': name, 'text': text})
        elif attrs != None:
            current['children'].append({'name': name, 'attrs': attrs})
        else:
            current['children'].append({'name': name})
    ################################################

    if 'children' in current:
        for children in current['children']:
            nodeTraversal(children, parent, name, text, attrs)


def getRowChildren(js):
    rowChildren = {
        'SNILS': None,
        'SURNAME': None,
        'NAME': None,
        'PATRONIMIC': None,
        'BOZ_FLAG': None,
        'LPU_EMPLOYER': None,
        'LN_CODE': None,
        'PREV_LN_CODE': None,
        'PRIMARY_FLAG': None,
        'DUPLICATE_FLAG': None,
        'LN_DATE': None,
        'LPU_NAME': None,
        'LPU_OGRN': None,
        'LPU_ADDRESS': None,
        'BIRTHDAY': None,
        'GENDER': None,
        'REASON1': None,
        'REASON2': None,
        'REASON3': None,
        'DIAGNOS': None,
        'PARENT_CODE': None,
        'VOUCHER_NO': None,
        'VOUCHER_OGRN': None,
        'SERV1_AGE': None,
        'SERV1_MM': None,
        'SERV1_RELATION_CODE': None,
        'SERV1_FIO': None,
        'SERV2_AGE': None,
        'SERV2_MM': None,
        'SERV2_RELATION_CODE': None,
        'SERV2_FIO': None,
        'LN_STATE': None
    }

    misbus.log("Загружаю JSON")

    misbus.log("Сортирую по словарям")
    for word in js:
        if js[word] != "":
            if word in rowChildren:
                rowChildren[word] = js[word]

    rowChildren['SNILS'] = re.sub(r'\-*\s*', rowChildren['SNILS'], '')
    return rowChildren

def getBreach(js):
    if "HOSPITAL_BREACH" in js:
        return {
            'name': 'HOSPITAL_BREACH',
            'wsuid': 'ELN_'+js['LN_CODE']+'_1_doc',
            'children': [
                    {'name': 'HOSPITAL_BREACH_COD', 'text': js['HOSPITAL_BREACH'][0]['HOSPITAL_BREACH_COD']},
                    {'name': 'HOSPITAL_BREACH_DT', 'text': js['HOSPITAL_BREACH'][0]['HOSPITAL_BREACH_DT']},
                ]
        }
    else:
        return None
    
def getPeriodsAndSignatures(childList, lncode, breach=None):
    signatures = [{'desc': 'Подпись МО', 'wsuid': 'ELN_' + lncode,
                   'actor': 'http://eln.fss.ru/actor/mo/1025601029563' + lncode}]
                   
    if breach!=None:
        signatures.append({'desc': 'Подпись Врача о нарушении режима ', 'wsuid': breach['wsuid'], 'actor': 'http://eln.fss.ru/actor/doc/' + lncode + '_1_doc' })

    list = {
        'name': "TREAT_PERIODS",
        'children': []
    }

    for i, child in enumerate(childList):
        dic = {
            'name': 'TREAT_FULL_PERIOD'
        }

        dic['children'] = []

        # если указан председатель
        if 'TREAT_CHAIRMAN' in child:
            # айди подписываемого председателем блока
            dic['wsuid'] = 'ELN_' + lncode + '_' + str(2 + i) + '_vk'

            signatures.append({'desc': 'Подпись председателя', 'wsuid': dic['wsuid'],
                               'actor': 'http://eln.fss.ru/actor/doc/' + lncode + '_' + str(2 + i) + '_vk'})

            dic['children'].append({
                'name': 'TREAT_CHAIRMAN',
                'text': child['TREAT_CHAIRMAN']
            })

            dic['children'].append({
                'name': 'TREAT_CHAIRMAN_ROLE',
                'text': child['TREAT_CHAIRMAN_ROLE']
            })
        dic['children'].append({
            'name': 'TREAT_PERIOD',
            'wsuid': 'ELN_' + lncode + '_' + str(2 + i) + '_doc',
            'children': [
                {'name': 'TREAT_DT1', 'text': child['TREAT_PERIOD']['TREAT_DT1']},
                {'name': 'TREAT_DT2', 'text': child['TREAT_PERIOD']['TREAT_DT2']},
                {'name': 'TREAT_DOCTOR_ROLE', 'text': child['TREAT_PERIOD']['TREAT_DOCTOR_ROLE']},
                {'name': 'TREAT_DOCTOR', 'text': child['TREAT_PERIOD']['TREAT_DOCTOR']},
            ]
        })
        signatures.append({'desc': 'Подпись врача', 'wsuid': 'ELN_' + lncode + '_' + str(2 + i) + '_doc',
                           'actor': 'http://eln.fss.ru/actor/doc/' + lncode + '_' + str(2 + i) + '_doc'})

        list['children'].append(dic)

    return list, signatures


def getTreatChildren(js):
    return js['TREAT_PERIODS']


def retrieveDictionary(rowChildren, treatChildren, breach = None):
    misbus.log("Составляю основной словарь")
    dictionary = {
        'name': 'prParseFilelnlpu',
        'attrs': {'xmlns': '"http://ru/ibs/fss/ln/ws/FileOperationsLn.wsdl"'},
        'children':
            [
                {
                    'name': 'request',
                    'children':
                        [
                            {
                                'name': 'ogrn',
                                'text': '1025601029563'
                            },
                            {
                                'name': 'pXmlFile',
                                'children':
                                    [
                                        {
                                            'name': 'ROWSET',
                                            'attrs':
                                                {

                                                    'version': '1.1',
                                                    'software': 'mdgkb_orb',
                                                    'version_software': '0.12',
                                                    'author': 'ibus test',
                                                    'phone': '88005553535',
                                                    'email': ''
                                                },

                                            'children':
                                                [
                                                    {
                                                        'name': 'ROW',
                                                        'wsuid': 'ELN_' + rowChildren['LN_CODE'],
                                                        'attrs': {'Id': ''},
                                                        'children': [

                                                        ]

                                                    }
                                                ]

                                        }
                                    ]
                            }
                        ]
                }
            ]
    }

    misbus.log("Объединяю ROW")
    for child in rowChildren:
        if rowChildren[child] != None:
            createNode(dictionary, child, parent='ROW', text=rowChildren[child], attributes=None)

    if breach!=None:
        misbus.log('Добавляю нарушение режима')
        dictionary['children'][0]['children'][1]['children'][0]['children'][0]['children'].append(breach)
        
    misbus.log("Объединяю ROW")

    dictionary['children'][0]['children'][1]['children'][0]['children'][0]['children'].append(treatChildren)

    return dictionary


def create(dictionary):
    misbus.log("Подключаюсь к ФСС")
    remote = misbus.get_external_remote(10)  # ФСС - Тестовый контур без шифрования
    oper = remote.get_operation("prParseFilelnlpu")
    misbus.log("Создаю XML")
    xml = oper.build_input_envelope(dictionary)
    return xml


def getXML(json_string):
    try:
        js = json.loads(json_string)
        
        #берем всех первых потомков row
        row = getRowChildren(js)
        
        #берем нарушение режима 
        breach = getBreach(js)
        
        #берем периоды и подписи
        periods, signatures = getPeriodsAndSignatures(js["TREAT_PERIODS"], js["LN_CODE"], breach)
        
        dic = retrieveDictionary(row, periods, breach)
        xml = create(dic)

        misbus.log("Подключась к базе")
        db = misbus.get_internal_remote(2)

        mis = misbus.import_script('integrations.mis')
        table = mis.TablePair(db, '_d_Netrud_Osvob_fss')
        misbus.log("Формирую ID")
        
        lastID = mis.generate_id()

        misbus.log("Создаю подписи")
        misbus.log("Создаю страницу запроса подписей")

        rel_url = misbus.request_signatures(
            description="Получение нового номера ЛН",
            xml=xml,
            signatures_data=signatures,
            final_script=misbus.get_script(86),  # Сценарий отправки подписанного запроса
            xml_param="xml",
            other_params={'newID': str(lastID)}
        )

        misbus.log("Вставляю в базу первичные данные")
        table.update({
            'ID': lastID,
            'LN_CODE': js['LN_CODE'],
            'NAMEURL': 'http://ibus.dgkb.lan' + rel_url,
            'DOP': 'Создана XML'
        }, ['ID'])

        # Ссылка на страницу подписывания отображается в выводе сценария
        return 'http://ibus.dgkb.lan' + rel_url;
    except Exception as Ex:
        script_output['er'] = traceback.format_exc()

