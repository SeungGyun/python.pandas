class CsvUtil:
    def csv_header(path):
        f = None
        try:
            f = open(path, 'r',encoding='UTF-8')
            line = f.readline()
            #print(len(f.readline()))
            return line.replace('"','').replace('\n','').replace('\r','').replace('\r\n','').split(",")
        except Exception as exc:            
            raise Exception('CsvUtil:csv_header 해더를 읽는데 실패함 : '+ str(exc))
        finally:
            if f:
                f.close()
