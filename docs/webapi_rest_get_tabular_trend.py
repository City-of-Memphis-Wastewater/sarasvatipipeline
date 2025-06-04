'''
Source: https://emerson.sharepoint.com/sites/EDSSupportCostaRicaTeam/Shared%20Documents/Forms/AllItems.aspx?id=%2Fsites%2FEDSSupportCostaRicaTeam%2FShared%20Documents%2FGeneral%2FArchivos%20Clientes%2FNC%2D2501%2D6447%20%2D%2D%20Geoge%20Bennet%20%2D%2D%20Memphis%2FTT%20files&p=true&ga=1
Contact: Geovanny Martinez <Geovanny.Martinez@Emerson.com>
Ticket ID: NC-2501-6447
Date: May 14th
Modfied by: patryk.olchanowski@ttas.pl


Email Record:
    From: Martinez, Geovanny [EMR/SYSS/PWS/SJO] <Geovanny.Martinez@Emerson.com>
    Sent: Wednesday, May 14, 2025 11:00:38 AM
    To: BENNETT, GEORGE <GEORGE.BENNETT@memphistn.gov>; PWS Ovation Product Support [PROCESS/PWS/PITT] <Ovationproductsupport@Emerson.com>
    Cc: Ortega, Jose [EMR/SYSS/PWS/SJO] <jose.ortega@emerson.com>; Coronel, Luis Fernando [EMR/SYSS/PWS/SJO] <LuisFernando.Coronel@Emerson.com>
    Subject: RE: Ovation Call NC-2501-6447 Inquiry about the EDS API package [Ref:NC-2501-0886]
    Hello George,

    Based on the sent code and your comments, this is the response from our EDS Developers:

    From the code, it looks like they are trying to fetch the trend result immediately after sending the request. However, querying historical data may take any amount of time - from a few seconds to even several minutes.

    For all such requests that return a requestId, the proper approach is to check the task status using the following request periodically:

    GET /api/v1/requests?id=<requestId>

    Only once the result is ready should it be retrieved.
'''


from datetime import datetime
import time
import requests


API_URL = 'http://127.0.0.1:43084/api/v1/'
POINTS = ['test1', 'test2']



def login():
    session = requests.Session()

    data = {'username': 'admin', 'password': '', 'type': 'script'}
    res = session.post(API_URL + 'login', json=data, verify=False).json()
    session.headers['Authorization'] = 'Bearer ' + res['sessionId']

    return session


def create_tabular_request(session):
    data = {
        'period': {
            'from': int(datetime(2024, 12, 16, 15).timestamp()),
            'till': int(datetime(2024, 12, 16, 18).timestamp()),
        },
        'step': 600,
        'items': [{
            'pointId': {'iess': p},
            'shadePriority': 'DEFAULT',
            'function': 'AVG'
        } for p in POINTS],
    }
    res = session.post(API_URL + 'trend/tabular', json=data, verify=False).json()
    return res['id']


def wait_for_request_execution(session, req_id):
    st = time.time()
    while True:
        time.sleep(1)
        res = session.get(f'{API_URL}requests?id={req_id}', verify=False).json()
        status = res[str(req_id)]
        if status['status'] == 'FAILURE':
            raise RuntimeError('request [{}] failed: {}'.format(req_id, status['message']))
        elif status['status'] == 'SUCCESS':
            break
        elif status['status'] == 'EXECUTING':
            print('request [{}] progress: {:.2f}\n'.format(req_id, time.time() - st))

    print('request [{}] executed in: {:.3f} s\n'.format(req_id, time.time() - st))


def get_tabular(session, req_id):
    results = [[] for _ in range(len(POINTS))]
    while True:
        response = session.get(f'{API_URL}trend/tabular?id={req_id}', verify=False).json()
        for chunk in response:
            if chunk['status'] == 'TIMEOUT':
                raise RuntimeError('timeout')

            if chunk['status'] == 'LAST':
                return results

            for idx, samples in enumerate(chunk['items']):
                results[idx] += samples


session = login()
req_id = create_tabular_request(session)
wait_for_request_execution(session, req_id)
results = get_tabular(session, req_id)

session.post(API_URL + 'logout', verify=False)

for idx, iess in enumerate(POINTS):
    print('\n{} samples:'.format(iess))
    for s in results[idx]:
        print('{} {} {}'.format(datetime.fromtimestamp(s[0]), s[1], s[2]))
