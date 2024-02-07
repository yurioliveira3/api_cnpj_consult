from datetime import datetime, timezone
import http.client
import psycopg2
import json
import time 

class CNPJ_Collector:
    #INSTANCE DB CONNECTION AND CURSOR
    #def __init__(self, host, database, user, password, port):
    def __init__(self, host='127.0.0.1', database='postgres', user='postgres', password='postgres', port='5432'):
        self.connection = psycopg2.connect('host= {0} dbname= {1} user= {2} password= {3} port = {4}'.format(host, database, user, password, port))
        self.connection.autocommit = True
        self.cursor = self.connection.cursor()
        
    #EXECUTE GET REQUEST
    def get_api_cnpj(self, cnpj):
        self.cnpj = cnpj
        conn = http.client.HTTPSConnection("api.cnpja.com")
        headers = {'Authorization': 'API_TOKEN'}

        try:
            conn.request("GET", "/office/{0}".format(self.cnpj),'', headers)

            response = conn.getresponse()

            if response.code == 200: #success
                api_ret = response.read().decode("utf-8")
            else:
                api_ret = ''
        
            return api_ret         
            
        except Exception as e:
            print("Error while get api response", e)
            return ''      

    #REFRESH MATERIALIZED VIEW, TO GET NEW CLIENTES TO COLLECT
    def refresh_materialized_view(self):
        try:
            self.cursor.execute("REFRESH MATERIALIZED VIEW api_cnpj_collect;")

        except Exception as e:
            print("Error while refresh materialized view", e)

    #GET CNPJ TO SEARCH 
    def get_tx_clients(self):
        try:
            self.cursor.execute("SELECT tx_id FROM api_cnpj_collect;")
            return self.cursor.fetchall()

        except Exception as e:
            print("Error while get tx clients", e)

    #CONSOLIDATE NEW DATA IN DATABASE
    def api_consolidate_data(self, tx_clients):
        try:
            self.tx_clients = tx_clients

            insert_values = []

            query = '''
                        INSERT INTO consolidated_api_clients
                        (
                            "taxId",
                            "company",
                            "alias",
                            "founded",
                            "mainActivity",
                            "sideActivities",
                            "head",
                            "status",
                            "statusDate",
                            "address",
                            "phones",
                            "emails",
                            "updated"
                        ) 
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                    '''

            tx_len = len(tx_clients)

            for idx, tx in enumerate(tx_clients):
                cnpj = self.get_api_cnpj(tx[0])

                if cnpj and cnpj != '':
                    cnpj = json.loads(cnpj)

                    print("[{0}/{1}] Collecting - Tx: {2}...".format(idx+1,tx_len,cnpj['taxId']))

                    values = (
                                str(cnpj['taxId']),
                                str(cnpj['company']),
                                str(cnpj['alias']),
                                datetime.strptime(cnpj['founded'], '%Y-%m-%d').date(),
                                str(cnpj['mainActivity']),
                                str(cnpj['sideActivities']),
                                bool(cnpj['head']),
                                str(cnpj['status']),
                                datetime.strptime(cnpj['statusDate'], '%Y-%m-%d').date(),
                                str(cnpj['address']),
                                str(cnpj['phones']),
                                str(cnpj['emails']),
                                datetime.strptime(str(cnpj['updated']), '%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=timezone.utc),
                            )
                    
                    insert_values.append(values)

                time.sleep(7)  

            if len(insert_values) != 0:
                self.cursor.executemany(query, insert_values)              

            print("{0} - Rows Inserted!".format(self.cursor.rowcount))

            self.cursor.close()
            self.cursor.close() 

        except Exception as e:
                print("Error while insert data", e)    

def main():
        print('-'*60)
        print("CNPJ Collector API")
        print('-'*60)
    
        print(f'Ini - {datetime.now()}')

        cnpj_collector = CNPJ_Collector()

        cnpj_collector.refresh_materialized_view()

        tx_clients = cnpj_collector.get_tx_clients()

        cnpj_collector.api_consolidate_data(tx_clients)

        print(f'End - {datetime.now()}')
        
if __name__ == "__main__":
    main()
