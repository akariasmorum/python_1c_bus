from System import DateTime
import traceback
import base64
import clr

db = misbus.get_internal_remote(2)
dbOper = misbus.import_script('Common db Operations')
mis = misbus.import_script('integrations.mis')
table = mis.TablePair(db, '_d_Netrud_Osvob_fss')
tableD = mis.TablePair(db, '_d_Netrud_Osvob_fss_d')
fss = misbus.get_external_remote(10)


def deleteOriginal(db, data):
    try:
        idx = mis.generate_id()
        dictionary = {
            'ID': idx,
            'ID1': idx,
            'DATETIME_C': DateTime.Now,
            'ID_D': data[0],
            'ID_SERVER_D': data[2],
            'ID_LOGIN_D': data[3],
            'DATETIME_D': data[4],
            'TYPE_D': 1,
            'NAMEURL': data[6],
            'LN_CODE': data[7],
            'FL_SEND': data[14],
            'FL_ERROR': data[15],
            'DOP': data[16],
        }
        dbOper.insertDataInTable(db, '_d_Netrud_Osvob_fss_d', dictionary)
        dbOper.deleteDataFromTable(db, '_d_Netrud_Osvob_fss', id)
    except Exception as ex:
        script_output['error in deleteOriginal'] = str(ex)


def updateData(xml, oldData):
    try:
        # собираю новые данные
        newData = {
            'ID': oldData['ID'],
            'ID1': oldData['ID'],
            'ID_SERVER': str(db.execute_query('select top 1 ID_Server1 from _a_Option ').data[0][0]),
            'ID_LOGIN': str(db.execute_query('select id from _s_user where kod={0}'.format(1000)).data[0][0]),
            'DATETIME_C': DateTime.Now,
            'NAMEURL': oldData['NAMEURL'],
            'LN_CODE': oldData['LN_CODE'],
            'FL_SEND': xml.select('//STATUS').text,
            'DOP': ''
        }

        # формирую DOP
        if xml.select('//STATUS').text == '0':
            newData['FL_ERROR'] = 1
            newData['DOP'] = 'Ошибка: {0} : '.format(xml.select('//MESS').text)
            for i, e in enumerate(xml.select_all('//ERR_MESS')):
                newData['DOP'] += str(i) + ': ' + e.text + ' / '
        # если статус 1, то успешно доставлено
        else:
            newData['DOP'] = 'Успешно доставлено в ФСС'

        # удаляю старую строку
        db.execute_query('DELETE FROM _d_Netrud_Osvob_fss WHERE ID=@ID', ID=oldData['ID'])

        # вставить новую строку
        table.update(newData, ['ID'])

        # добавить информацию об удаленной
        idx = mis.generate_id()
        tableD.update({
            'ID': idx,
            'ID1': idx,
            'DATETIME_C': DateTime.Now,
            'ID_D': oldData['ID'],
            'ID_SERVER_D': oldData['ID_SERVER'],
            'ID_LOGIN_D': oldData['ID_LOGIN'],
            'DATETIME_D': oldData['DATETIME_C'],
            'TYPE_D': 1,
            'NAMEURL': oldData['NAMEURL'],
            'LN_CODE': oldData['LN_CODE'],
            'FL_SEND': oldData['FL_SEND'],
            'FL_ERROR': oldData['FL_ERROR'],
            'DOP': oldData['DOP'],
        }, ['ID'])


    except Exception as Ex:
        script_output['error updating data'] = traceback.format_exc()


def sendXML(xml):
    try:
        encrypted_request = misbus.encrypt_xml(xml, "Фонд", "ДГКБ")
        oper = fss.get_operation("prParseFilelnlpu")

        encrypted_response = oper.execute(encrypted_request)

        decrypted_response = misbus.decrypt_xml(encrypted_response, "ДГКБ")
        return decrypted_response
    except Exception as sendEx:
        script_output['sending xml error str'] = str(sendEx)
        script_output['sending xml error'] = traceback.format_exc()
        script_output['sending xml error xml'] = str(xml)
        script_output['sending xml error encrypted'] = encrypted_request


try:
    xmlInput = script_input['xml']
    response = sendXML(xmlInput)
    #######подключение к базе
    parsedXML = misbus.parse_xml(response)

    newid1 = bytes(script_input['newID'])

    oldData = table.select({'ID': newid1})[0]
    updateData(parsedXML, oldData)

    script_output['xml'] = response

except Exception as exc:

    script_output['Какая-то общая ошибка'] = traceback.format_exc()