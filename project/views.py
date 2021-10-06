from django.shortcuts import render
from django.http import HttpResponse
from django.http import JsonResponse
import mysql.connector as mysql
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
    database = connectToMySQL()
    value = {}
    if database is None:
        value['database'] = ''
    else:
        value['database'] = database
    return render(request, 'index.html', value)


def connectToMySQL():
    cursor = connections['redshift'].cursor()
    cursor.execute('select database()')
    result = cursor.fetchone()[0]
    return result


def execute_sql(sql, conn):
    """
    :param sql: sql query
    :param conn: database connection instance
    :return: query result
    """
    cursor = conn.cursor()
    cursor.execute(sql)
    result = cursor.fetchall()
    return result


# receive ajax request and return requested data
def ajax(request):
    databaseType = request.POST.get('databaseType')
    currentDatabase = request.POST.get('currentDatabase')
    query = request.POST.get('query')
    print('Received Query: ', query)

    startTime = time.time()
    cursor = connections['mysql'].cursor()
    influencedRow = cursor.execute(query)
    endTime = time.time()

    fetchResult = cursor.fetchall()

    result = None
    if influencedRow == 1:
        result = "%d row retrieved\n" % influencedRow
    else:
        result = "%d rows retrieved\n" % influencedRow
    result += str(fetchResult)
    print('Result: ', result)

    cursor.execute('select database()')
    currentDatabase = cursor.fetchone()[0]
    return JsonResponse({'result': result, 'executionTime': str(round(endTime - startTime, 2)) + ' s',
                         'currentDatabase': str(currentDatabase)})
