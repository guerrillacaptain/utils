import itertools
import operator
import sys
import logging
import logging.handlers
#logging.basicConfig(format='%(asctime)s:%(levelname)s:%(message)s', level=logging.INFO)
formatter = logging.Formatter('%(levelname)-8s %(name)-12s %(asctime)s %(process)d [%(module)s:%(lineno)s %(funcName)s] %(message)s',  '%Y-%m-%d %H:%M:%S')
file_handler = logging.handlers.RotatingFileHandler('server.log',maxBytes=102400000, backupCount=10)
file_handler.setFormatter(formatter)
logger = logging.getLogger('')
logger.addHandler(file_handler)
logger.setLevel(logging.INFO)

def get_pair_info(data):
    '''census the pair_num and inverse_pair_num of the data list who's element is (pctr,label)
    '''
    def get_combination_num(n,k):
        return reduce(operator.mul, range(n - k + 1, n + 1)) / reduce(operator.mul, range(1, k +1))                                        
    def get_inverse_combination_num(temp_list):
        return sum(1 for x, y in itertools.combinations(xrange(len(temp_list)), 2) if temp_list[x][1] > temp_list[y][1] and (temp_list[x][1] > 0 or temp_list[y][1] > 0))

    ## sort and compute pair num ,inverse_pair num
    data.sort(key=lambda t:(t[0], -t[1]))
    ##plus the sample label pair_num C(n,2)-C(k,2)-C(l,2) 
    pair_num = get_combination_num(len(data), 2) - get_combination_num(len([1 for j in data if j[1] == -1 ]), 2) - get_combination_num(len([1 for j in data if j[1] == 1 ]), 2)
    inverse_pair_num = get_inverse_combination_num(data)

    return pair_num,inverse_pair_num

def get_ranking_score(pctr, pgmv, pvalue, a, b, t):
    bid = 0.01
    x = a*(b*bid+(1-b)*pvalue)+(1-a)*pgmv
    return pctr**t*x

def get_pair_info_by_bucket(data, bucket_size, bucket_num):
    '''census the pair_num and inverse_pair_num of the data list within bucket who's element is (pctr,label)
    '''
    data.sort(key=lambda t:(-t[0], -t[1]))
    #logger.info("data: %s" % str(data))
    bucket = []
    negative_num = 0
    for i in range(bucket_num):
        bucket.append(dict())
        bucket[i]["positive"] = 0

    #logger.info("bucket:%s "% str(bucket))
    for i in range(len(data)):
        bucket_no = i / bucket_size

        if bucket_no < bucket_num:
            #logger.info(data[i])
            if data[i][1] == 1:
                bucket[bucket_no]["positive"] += 1
            else:
                negative_num += 1
        else:
            break

    #logger.info("bucket: %s" % str(bucket))
    pair_num = 0
    inverse_pair_num = 0
    before_negative_num = 0
    for i in range(bucket_num):
        pair_num +=  bucket[i]["positive"]* (negative_num - (bucket_size - bucket[i]["positive"]))
        inverse_pair_num += bucket[i]["positive"] * before_negative_num
        before_negative_num += bucket_size - bucket[i]["positive"]
    
    #logger.info("pair_num:%d, inverse_pair_num:%d" % (pair_num, inverse_pair_num))
    return pair_num, inverse_pair_num

def test_get_pair_info():
    data = [(0.0015344200000000001, -1), (0.00176348, -1), (0.00186864, -1), (0.0018716900000000001, -1), (0.00244437, -1), (0.0024478099999999999, -1), (0.0028120699999999998, -1), (0.0050319199999999996, -1), (0.0068870399999999997, -1), (0.0079058800000000005, -1), (0.0090277099999999996, -1), (0.0091137200000000005, -1), (0.0108713, -1), (0.011799799999999999, -1), (0.0130346, -1), (0.014038800000000001, 1), (0.0146626, -1), (0.014877100000000001, -1), (0.020239400000000001, -1), (0.021520899999999999, -1), (0.0268988, -1), (0.054637499999999999, -1)]
    data_1 = [(0.0015344200000000001, -1), (0.00176348, -1), (0.00186864, -1), (0.0018716900000000001, -1), (0.00244437, -1), (0.0024478099999999999, -1), (0.0028120699999999998, -1), (0.0050319199999999996, -1), (0.0068870399999999997, -1), (0.0079058800000000005, -1), (0.0090277099999999996, -1), (0.0091137200000000005, -1), (0.0108713, -1), (0.011799799999999999, -1), (0.0130346, -1), (0.014038800000000001, 1), (0.0146626, 1), (0.014877100000000001, -1), (0.020239400000000001, -1), (0.021520899999999999, -1), (0.0268988, -1), (0.054637499999999999, -1)]
    logger.info("pair_num:%d, inverse_pair_num:%d" % get_pair_info(data))
    logger.info("pair_num:%d, inverse_pair_num:%d" % get_pair_info(data_1))

def test_get_pair_info_by_bucket():
    data = [(0.0015344200000000001, -1), (0.00176348, -1), (0.00186864, -1), (0.0018716900000000001, -1), (0.00244437, -1), (0.0024478099999999999, -1), (0.0028120699999999998, -1), (0.0050319199999999996, -1), (0.0068870399999999997, -1), (0.0079058800000000005, -1), (0.0090277099999999996, -1), (0.0091137200000000005, -1), (0.0108713, -1), (0.011799799999999999, -1), (0.0130346, -1), (0.014038800000000001, 1), (0.0146626, -1), (0.014877100000000001, -1), (0.020239400000000001, -1), (0.021520899999999999, -1), (0.0268988, -1), (0.054637499999999999, -1)]
    data_1 = [(0.0015344200000000001, -1), (0.00176348, -1), (0.00186864, -1), (0.0018716900000000001, -1), (0.00244437, -1), (0.0024478099999999999, -1), (0.0028120699999999998, -1), (0.0050319199999999996, -1), (0.0068870399999999997, -1), (0.0079058800000000005, -1), (0.0090277099999999996, -1), (0.0091137200000000005, -1), (0.0108713, -1), (0.011799799999999999, -1), (0.0130346, -1), (0.014038800000000001, 1), (0.0146626, 1), (0.014877100000000001, -1), (0.020239400000000001, -1), (0.021520899999999999, -1), (0.0268988, -1), (0.054637499999999999, -1)]
    data_2 = [(0.0015344200000000001, -1), (0.00176348, 1), (0.00186864, -1), (0.0018716900000000001, 1), (0.00244437, -1), (0.0024478099999999999, -1), (0.0028120699999999998, -1), (0.0050319199999999996, -1), (0.0068870399999999997, -1), (0.0079058800000000005, -1), (0.0090277099999999996, -1), (0.0091137200000000005, -1), (0.0108713, -1), (0.011799799999999999, -1), (0.0130346, -1), (0.014038800000000001, 1), (0.0146626, 1), (0.014877100000000001, -1), (0.020239400000000001, -1), (0.021520899999999999, -1), (0.0268988, -1), (0.054637499999999999, -1)]
                    
    logger.info("pair_num:%d, inverse_pair_num:%d" % get_pair_info_by_bucket(data, 4, 5))
    logger.info("pair_num:%d, inverse_pair_num:%d" % get_pair_info_by_bucket(data_1, 4, 5))
    logger.info("pair_num:%d, inverse_pair_num:%d" % get_pair_info_by_bucket(data_2, 4, 5))

if __name__ == '__main__':
    #test_get_pair_info()
    #test_get_pair_info_by_bucket()
    #sys.exit(0)
    #file = "old_predict.txt"
    file = "app_predict.txt"
    pctr_pair_num = 0
    pctr_inverse_pair_num = 0
    
    for line in open(file):
        sid, value = line.split('\t', 1)
        pctr_pair = []
        for pair in value.split('\t'):
            pctr_pair.append((float(pair.split('_')[0]),int(pair.split('_')[1])))
        if len(pctr_pair) < 20:
            continue

        pair_num, inverse_pair_num = get_pair_info_by_bucket(pctr_pair, 4, 5)
        pctr_pair_num += pair_num
        pctr_inverse_pair_num += inverse_pair_num
    print "pctr pair_num:%d,inverse_pair_num:%d" % (pctr_pair_num, pctr_inverse_pair_num)
