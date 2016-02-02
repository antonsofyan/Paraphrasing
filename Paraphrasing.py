# !/usr/bin/env python
# -*- coding:utf-8 -*-

import sys, gzip, sqlite3, copy, multiprocessing, click
from multiprocessing import Process, Queue

threshold = 0.01
batchsize = 1000
hashsize = 48

def hash_single(hash_no, queue):
    with sqlite3.connect('./tmp/hash'+str(hash_no)+'.db') as conn:
        curs = conn.cursor()
        index = list(set(curs.execute('select f from alignment').fetchall()))
        
        for i in index:
            for q in make_single(AlignmentIter(curs, i)):
                queue.put(q)
        queue.put('<HASH_SINGLE END SIGNAL>')

def make_single(alignments):

        result = []
        for e_f in alignments:
                for f_e in alignments:
                        # store a alignment data 
                        result.append((e_f[0],
                                                        e_f[1],
                                                        f_e[0],
                                                        float(e_f[4] * f_e[2]),
                                                        float(e_f[5] * f_e[3]), 
                                                        float(f_e[4] * e_f[2]),
                                                        float(f_e[5] * e_f[3]),))
        return result

def ResultIter(cursor, arraysize=10000):
        while True:
                results = cursor.fetchmany(arraysize)
                if not results:
                        break
                for result in results:
                        yield result

def AlignmentIter(cursor, index):
        return cursor.execute('select * from alignment where f = ?', index).fetchall()

if __name__=='__main__' :

        print("Initializing hash database")
        curs = [ None ] * hashsize
        conn = [ None ] * hashsize
        for i in range(hashsize):
            conn[i] = sqlite3.connect('./tmp/hash'+str(i)+'.db')
            curs[i] = conn[i].cursor()
            curs[i].execute('create table alignment (e TEXT, f TEXT, p_e_f REAL, p_e_f_m REAL, p_f_e REAL, p_f_e_m REAL)')

        queries = []
        for i in range(0, hashsize):
            queries.append([])
        
        print('Reading a alignment file (.gz)')
        with click.progressbar(gzip.open(sys.argv[1],'rb').readlines()) as _p:
            for i, line in enumerate(_p):
                data = line.decode('utf-8').split('|||')
                data_p = data[2].strip().split()

                hash_no = hash(data[1]) % hashsize
                #print(data)
                # store a alignment data 
                #print(hash(data[1]) % thread_num)
                queries[hash_no].append((data[0].strip(),
                                                                data[1].strip(),
                                                                float(data_p[0]),
                                                                float(data_p[1]),
                                                                float(data_p[2]),
                                                                float(data_p[3]),))
                if len(queries[hash_no]) > 1000:
                    curs[hash_no].executemany('insert into alignment values (?,?,?,?,?,?)', queries[hash_no])
                    queries[hash_no] = []
            else:
                for i in range(0, hashsize):
                    if len(queries[i]) > 0:
                        curs[i].executemany('insert into alignment values (?,?,?,?,?,?)', queries[i])
        # release
        queries = []
        for i in range(hashsize):
            conn[i].commit()
            conn[i].close()

        print('Making single-pivoted-paraphrase')
        
        with sqlite3.connect(':memory:') as conn:
            cursor = conn.cursor()
            cursor.execute('create table single (e TEXT, p TEXT, f TEXT, p_e_f REAL, p_e_f_m REAL, p_f_e REAL, p_f_e_m REAL)')
            

            queue = Queue()
            for i in range(0, hashsize):
                p = Process(target=hash_single, args=(i, queue,))
                p.start()
            
            es_count = 0
            count = 0
            while 1:
                print(str(count), end='\r')
                try:
                    if es_count == hashsize:
                        break
                    q = queue.get()
                    #print(q)
                    if q == '<HASH_SINGLE END SIGNAL>':
                        es_count += 1
                        print('End of hash single :' +str(es_count)+' / '+str(hashsize))
                        continue
                    else:
                        queries.append(q)
                        count += 1
                    
                    if len(queries) > 10000:
                        cursor.executemany('insert into single values (?,?,?,?,?,?,?)', queries)
                        queries = []
                    #cursor.execute('insert into single values (?,?,?,?,?,?)', q)

                except:
                    pass

            print('Marginalize single-pivoted-paraphrases')
            # marge single-paraphrases
            with gzip.open('./paraphrase.gz', 'wb') as _p :
                        e = ''
                        f = ''
                        p = ''
                        p_p = 0.0
                        p_e_f = 0.0
                        p_e_f_m = 0.0
                        p_f_e = 0.0
                        p_f_e_m = 0.0
                        for single in ResultIter(cursor.execute('select * from single order by e, f asc')):
                                #print(single)
                                if e != single[0] or f != single[2]:
                                        #print(e+' ||| '+f+' ||| ||| '+str(p_e_f)+' '+str(p_e_f_m)+' '+str(p_f_e)+' '+str(p_f_e_m))
                                        if p_e_f >= threshold :
                                                #print(e+' ||| '+f+' ||| ||| '+str(p_e_f)+' '+str(p_e_f_m)+' '+str(p_f_e)+' '+str(p_f_e_m))
                                                try:
                                                        _p.write( (e+' ||| '+f+' ||| ||| '+str(p_e_f)+' '+str(p_e_f_m)+' '+str(p_f_e)+' '+str(p_f_e_m)+' ||| '+p+'\n').encode('utf-8') )
                                                except Exception as e:
                                                        print(e)
                                        e = single[0]
                                        f = single[2]
                                        p_e_f = 0.0
                                        p_e_f_m = 0.0
                                        p_f_e = 0.0
                                        p_f_e_m = 0.0
                                
                                if single[3] > p_p :
                                    p = single[1]
                                    p_p = single[3]

                                p_e_f += single[3]
                                p_e_f_m += single[4]
                                p_f_e += single[5]
                                p_f_e_m += single[6]
        
        print('Finish all process')
        

