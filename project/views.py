import traceback

from django.shortcuts import render
# from django.http import HttpResponse
from django.http import JsonResponse
# import mysql.connector as mysql
import time
from django.db import connections


# def index(request):
#     return HttpResponse('project index page')

# connect to mysql by default when the project index page is requested for the first time
def index(request):
    """
    :param request: http request
    :return: index web page with data
    """
    return render(request, 'index.html')


def connectToDB(request):
    """
    :param request: http request
    :return: schema/database name
    """
    databaseType = request.POST.get('databaseType')
    responseStatus = 0      # 0 for no error, 1 for error
    cursor = None
    try:
        cursor = connections[databaseType].cursor()
    except Exception as e:
        responseStatus = 1
        traceback.print_exc()
        return JsonResponse({'responseStatus': responseStatus, 'errorDetails': str(e)})
    result = ''
    try:
        if databaseType == 'mysql':
            cursor.execute('select database()')
            result = cursor.fetchone()[0]
        elif databaseType == 'redshift':
            cursor.execute('select current_database()')
            result = cursor.fetchone()[0]
    except Exception as e:
        responseStatus = 1
        traceback.print_exc()
        return JsonResponse({'responseStatus': responseStatus, 'errorDetails': str(e)})

    return JsonResponse({'currentDatabase': result})


# receive ajax request and return requested data
def ajax(request):
    """
    :param request: http request
    :return: json response including query result
    """
    # received data
    databaseType = request.POST.get('databaseType')     # mysql/redshift/mongoDB
    currentDatabase = request.POST.get('currentDatabase')   # used database/schema
    query = str(request.POST.get('query')).replace('\n', '')    # input sql query
    query = [sql for sql in query.split(';') if sql]
    print('Received Query: ', query)

    # send data
    resultAttribute = []        # table attribute
    queryResult = []            # table data
    influencedRowResult = []    # number of rows influenced by a sql query
    totalTime = 0               # total time of executing all sql queries
    # currentDatabase           # return the current database/schema back in case it changed

    cursor = None
    try:
        cursor = connections[databaseType].cursor()
    except Exception as e:
        responseStatus = 1
        traceback.print_exc()
        return JsonResponse({'responseStatus': responseStatus, 'errorDetails': str(e)})

    for sql in query:
        startTime = time.time()
        try:
            cursor.execute(sql)
        except Exception as e:
            responseStatus = 1
            traceback.print_exc()
            return JsonResponse({'responseStatus': responseStatus, 'errorDetails': str(e)})
        endTime = time.time()
        totalTime += endTime - startTime
        influencedRow = cursor.rowcount
        # DML (INSERT, UPDATE) will use "affected", DQL (SELECT) will use "retrieved"
        try:
            if influencedRow == 0 or influencedRow == -1:
                influencedRowResult.append(None)
            elif influencedRow == 1:
                temp = f"{influencedRow} row "
                temp += "retrieved" if cursor.description else "affected"
                influencedRowResult.append(temp)
            else:
                temp = f"{influencedRow} rows "
                temp += "retrieved" if cursor.description else "affected"
                influencedRowResult.append(temp)
        except Exception as e:
            responseStatus = 1
            traceback.print_exc()
            return JsonResponse({'responseStatus': responseStatus, 'errorDetails': str(e)})
        try:
            if databaseType == 'mysql':
                queryResult.append(cursor.fetchall())
            elif databaseType == 'redshift':
                # no result will be fetched if the last query is DML (INSERT, UPDATE)
                if cursor.description:
                    # if cursor.description is not None, then it means the last query is DQL (SELECT)
                    queryResult.append(cursor.fetchall())
                else:
                    queryResult.append(())
        except Exception as e:
            responseStatus = 1
            traceback.print_exc()
            return JsonResponse({'responseStatus': responseStatus, 'errorDetails': str(e)})
        try:
            if cursor.description:
                resultAttribute.append([attribute[0] for attribute in cursor.description])
            else:
                resultAttribute.append(None)
        except Exception as e:
            responseStatus = 1
            traceback.print_exc()
            return JsonResponse({'responseStatus': responseStatus, 'errorDetails': str(e)})

    try:
        for i in range(len(influencedRowResult)):
            print(f'Executed Query: {query[i]}')
            print(influencedRowResult[i])
            print(f'Result Attributes: {resultAttribute[i]}')
            print(f'Query Result: {queryResult[i]}')
            print()
        print(f'Total Execution Time: {round(totalTime, 2)} s')
    except Exception as e:
        responseStatus = 1
        traceback.print_exc()
        return JsonResponse({'responseStatus': responseStatus, 'errorDetails': str(e)})

    try:
        if databaseType == 'mysql':
            cursor.execute('select database()')
            currentDatabase = cursor.fetchone()[0]
        elif databaseType == 'redshift':
            cursor.execute('select current_database()')
            currentDatabase = cursor.fetchone()[0]
    except Exception as e:
        responseStatus = 1
        traceback.print_exc()
        return JsonResponse({'responseStatus': responseStatus, 'errorDetails': str(e)})

    return JsonResponse({'influencedRowResult': influencedRowResult, 'resultAttribute': resultAttribute,  'queryResult': queryResult, 'executionTime': str(round(totalTime, 2)) + ' s',
                         'currentDatabase': str(currentDatabase)})
