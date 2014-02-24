import math

mas = [None]*17

mas[0] = [[299.84933333333333, 233.0438666666667, 233.0438666666667, 283.6838666666667, 313.23320000000007], [321.25706666666673, 248.1610666666667, 321.1637333333334, 321.1637333333334, 321.1637333333334], [270.4516, 325.18533333333335, 270.4516, 270.4516, 270.4516], [292.74213333333336, 292.74213333333336, 292.74213333333336, 292.74213333333336, 325.4413333333334], [332.122, 315.03266666666667, 315.03266666666667, 315.03266666666667, 332.122], [337.32320000000004, 354.4125333333334, 354.4125333333334, 337.32320000000004, 337.32320000000004], [376.7030666666667, 359.61373333333336, 359.61373333333336, 385.0950666666667, 359.61373333333336], [407.3856000000001, 381.9042666666668, 381.9042666666668, 381.9042666666668, 381.9042666666668], [421.28413333333344, 421.2841333333335, 404.1948000000001, 404.1948000000001, 404.1948000000001]]

mas[1] = [[233.04240000000001, 283.68240000000003, 225.9624, 225.9624, 229.7690666666667], [248.15813333333335, 248.15813333333335, 321.25413333333336, 248.15813333333335, 321.25413333333336], [325.42666666666673, 325.4266666666667, 270.4472, 325.4266666666667, 270.4472], [292.73626666666667, 325.4266666666667, 292.73626666666667, 292.73626666666667, 325.4266666666667], [332.11466666666666, 315.0253333333334, 332.1146666666667, 315.0253333333334, 315.0253333333334], [337.31440000000003, 337.31440000000003, 337.31440000000003, 354.4037333333334, 337.31440000000003], [359.6034666666667, 359.6034666666667, 376.69280000000003, 359.6034666666667, 359.6034666666667], [398.98186666666675, 381.8925333333334, 381.8925333333334, 381.8925333333334, 398.98186666666675], [421.27093333333346, 404.1816000000002, 421.27093333333346, 421.27093333333346, 404.1816000000002]]

mas[2] = [[240.5948, 240.5948, 273.59479999999996, 283.98133333333334, 273.1681333333333], [260.41893333333337, 283.98133333333334, 260.41893333333337, 284.19466666666665, 284.19466666666665], [280.2937333333333, 291.18800000000005, 280.2937333333333, 280.2937333333333, 280.2937333333333], [284.19466666666665, 284.19466666666665, 284.19466666666665, 300.16853333333336, 284.19466666666665], [302.95400000000006, 302.95400000000006, 309.94733333333335, 307.10133333333346, 302.95400000000006], [329.82213333333334, 339.91813333333334, 322.8288, 322.82880000000006, 322.8288], [342.70360000000005, 342.70360000000005, 359.7929333333334, 342.70360000000005, 342.70360000000005], [362.5784000000001, 362.5784000000001, 362.5784000000001, 440.9557333333334, 362.5784000000001], [382.45320000000004, 399.5425333333334, 382.45320000000004, 382.45320000000004, 399.5425333333334]]

mas[3] = [[368.7733333333335, 325.82666666666677, 368.7733333333335, 325.82666666666677, 368.7733333333335], [325.82666666666677, 334.7653333333334, 374.9280000000001, 349.00533333333334, 325.82666666666677], [328.3973333333334, 380.1546666666668, 380.37466666666677, 385.0066666666668, 336.85333333333335], [397.1066666666668, 397.1066666666668, 404.1466666666668, 408.8853333333334, 404.00000000000017], [383.63333333333344, 404.11333333333346, 424.3066666666668, 404.1466666666668, 404.1466666666668], [424.71200000000005, 411.0600000000002, 411.0600000000002, 411.2266666666668, 411.2266666666668], [434.1800000000002, 435.61333333333346, 448.7480000000001, 431.65866666666676, 448.7480000000001], [481.33066666666684, 464.2413333333335, 464.24133333333344, 464.2413333333335, 481.33066666666684], [503.81733333333347, 496.8240000000001, 496.8240000000001, 513.9133333333334, 513.9133333333334]]

mas[4] = [[233.03093333333337, 225.95093333333335, 233.03093333333337, 233.03093333333337, 233.03093333333337], [248.13520000000003, 248.13520000000003, 321.1378666666667, 248.13520000000003, 321.23120000000006], [270.4128, 325.312, 325.312, 270.4128, 325.056], [292.6904, 292.6904, 292.6904, 292.6904, 292.6904], [314.968, 314.968, 314.968, 314.968, 332.0573333333333], [354.33493333333337, 354.33493333333337, 354.33493333333337, 337.2456, 337.2456], [385.00453333333337, 359.52320000000003, 359.52320000000003, 376.61253333333343, 376.6125333333334], [381.8008000000001, 398.89013333333344, 381.8008000000001, 381.8008000000001, 381.8008000000001], [404.07840000000004, 404.07840000000004, 404.07840000000004, 421.1677333333334, 404.07840000000004]]

mas[5] = [[290.996, 284.1959999999999, 290.38826666666665, 290.996, 291.1893333333333], [291.38266666666664, 261.85653333333335, 261.85653333333335, 291.16933333333327, 291.38266666666664], [291.38266666666664, 291.38266666666664, 282.4501333333333, 291.38266666666664, 291.38266666666664], [291.38266666666664, 291.38266666666664, 303.0437333333333, 303.0437333333333, 291.38266666666664], [313.54133333333334, 323.63733333333334, 306.548, 306.548, 306.548], [344.2309333333333, 327.1416, 327.1416, 344.2309333333333, 327.1416], [364.8245333333333, 347.73519999999996, 364.8245333333333, 364.8245333333333, 347.73519999999996], [368.3288, 368.3288, 385.41813333333334, 368.3288, 385.41813333333334], [406.0117333333334, 406.0117333333334, 388.92240000000004, 406.0117333333334, 406.0117333333334]]

mas[6] = [[233.04240000000001, 225.9624, 225.9624, 233.04240000000001, 308.27840000000003], [321.25413333333336, 248.15813333333335, 323.5493333333334, 323.576, 248.15813333333335], [325.1706666666667, 270.4472, 325.1706666666667, 270.4472, 325.1706666666667], [292.73626666666667, 292.73626666666667, 325.4266666666667, 292.73626666666667, 292.73626666666667], [332.11466666666666, 315.0253333333334, 315.0253333333334, 332.11466666666666, 315.0253333333334], [362.7957333333334, 337.31440000000003, 337.31440000000003, 337.31440000000003, 337.31440000000003], [376.69280000000003, 450.12346666666673, 376.69280000000003, 359.6034666666667, 359.6034666666667], [381.8925333333334, 381.8925333333334, 381.8925333333334, 381.8925333333334, 381.8925333333334], [404.1816000000002, 421.27093333333346, 404.1816000000002, 404.1816000000001, 404.1816000000001]]

mas[7] = [[242.038, 283.8466666666667, 242.038, 283.8466666666667, 242.038], [291.0266666666667, 291.0266666666667, 291.0266666666667, 291.0266666666667, 291.0266666666667], [291.0266666666667, 291.0266666666667, 291.0266666666667, 282.3433333333333, 291.0266666666667], [291.0266666666667, 302.90133333333335, 291.0266666666667, 291.0266666666667, 291.0266666666667], [306.37, 314.75666666666666, 314.75666666666666, 393.434, 323.45933333333335], [344.0173333333334, 326.928, 326.928, 333.92133333333334, 326.928], [364.5753333333334, 347.48600000000005, 364.5753333333334, 354.4793333333334, 364.5753333333334], [368.0440000000001, 385.13333333333344, 385.1333333333335, 368.0440000000001, 368.0440000000001], [480.96600000000007, 388.6020000000001, 395.59533333333343, 388.6020000000001, 405.69133333333343]]

mas[8] = [[283.65653333333336, 233.01653333333337, 313.2592000000001, 313.2258666666668, 233.01653333333337], [248.10640000000004, 248.10640000000004, 248.10640000000004, 248.10640000000004, 321.10906666666676], [270.36959999999993, 325.168, 270.36959999999993, 270.36959999999993, 270.36959999999993], [292.63280000000003, 292.63280000000003, 292.63280000000003, 292.63280000000003, 292.63280000000003], [331.9853333333333, 314.896, 331.98533333333336, 331.98533333333336, 331.9853333333333], [337.1592, 337.1592, 337.1592, 337.1592, 337.1592], [376.5117333333334, 359.42240000000004, 376.5117333333334, 359.42240000000004, 359.42240000000004], [381.6856, 398.77493333333337, 398.77493333333337, 381.6856, 398.77493333333337], [421.03813333333346, 403.94880000000006, 421.03813333333346, 403.94880000000006, 421.03813333333346]]

mas[9] = [[283.9, 284.11333333333334, 284.11333333333334, 242.06466666666668, 284.11333333333334], [261.83866666666665, 261.83866666666665, 261.83866666666665, 261.83866666666665, 291.08], [291.29333333333335, 291.29333333333335, 282.42333333333335, 291.29333333333335, 291.29333333333335], [291.29333333333335, 291.29333333333335, 303.008, 303.008, 298.2866666666667], [306.50333333333333, 314.2, 323.5926666666667, 323.5926666666667, 306.50333333333333], [327.088, 344.17733333333337, 344.17733333333337, 334.08133333333336, 327.088], [347.6726666666667, 354.66600000000005, 347.6726666666667, 364.76200000000006, 347.6726666666667], [385.3466666666667, 368.25733333333335, 368.25733333333335, 375.2506666666667, 375.2506666666667], [388.84200000000004, 388.84200000000004, 405.9313333333334, 388.84200000000004, 388.84200000000004]]

mas[10] = [[251.25920000000002, 243.22719999999998, 300.4858666666667, 251.25920000000002, 242.7632], [265.0064, 298.62533333333334, 273.30240000000003, 311.36, 273.30240000000003], [283.2, 305.83200000000005, 283.2, 305.83200000000005, 283.2], [308.8421333333334, 308.8421333333334, 305.8786666666667, 308.8421333333334, 308.8421333333334], [313.796, 313.796, 330.88533333333334, 330.88533333333334, 313.796], [335.83920000000006, 352.92853333333335, 335.83920000000006, 352.92853333333335, 335.83920000000006], [436.2597333333334, 374.97173333333336, 357.8824, 357.8824, 374.97173333333336], [379.92560000000003, 379.92560000000003, 379.92560000000003, 379.92560000000003, 379.92560000000003], [401.96880000000004, 419.0581333333334, 401.96880000000004, 419.0581333333334, 419.0581333333334]]

mas[11] = [[276.8533333333333, 249.06933333333336, 273.44, 283.93333333333334, 273.44], [260.37600000000003, 260.37600000000003, 283.76666666666665, 283.97999999999996, 260.37600000000003], [283.97999999999996, 280.22933333333333, 280.22933333333333, 280.22933333333333, 283.97999999999996], [300.0826666666667, 283.97999999999996, 283.97999999999996, 300.0826666666667, 284.38666666666666], [302.8466666666667, 309.84000000000003, 319.93600000000004, 309.84000000000003, 319.93600000000004], [322.7, 322.70000000000005, 322.7, 339.78933333333333, 329.6933333333333], [359.64266666666674, 342.5533333333334, 342.5533333333334, 349.54666666666674, 342.5533333333334], [362.40666666666675, 369.4000000000001, 362.40666666666675, 362.40666666666675, 362.40666666666675], [399.3493333333334, 382.26000000000005, 399.3493333333334, 382.26000000000005, 382.26000000000005]]

mas[12] = [[284.02799999999996, 290.54346666666663, 249.79946666666666, 249.79946666666666, 249.79946666666666], [261.8362666666667, 291.068, 291.28133333333335, 261.8362666666667, 291.28133333333335], [291.28133333333335, 282.41973333333334, 282.41973333333334, 282.41973333333334, 291.28133333333335], [291.28133333333335, 291.28133333333335, 291.28133333333335, 303.0032, 303.0032], [314.188, 306.49733333333336, 323.5866666666667, 306.49733333333336, 306.49733333333336], [334.07413333333335, 344.17013333333335, 344.17013333333335, 344.17013333333335, 344.17013333333335], [347.66426666666666, 364.7536, 364.7536, 364.7536, 364.7536], [368.2477333333334, 368.2477333333334, 385.3370666666667, 385.3370666666667, 368.2477333333334], [388.8312000000001, 388.8312000000001, 388.8312000000001, 405.9205333333334, 388.8312000000001]]

mas[13] = [[317.40360000000004, 287.06093333333337, 256.676, 316.708, 318.2302666666667], [333.62000000000006, 262.0885333333333, 333.82000000000005, 256.676, 333.62000000000006], [280.58279999999996, 280.58279999999996, 353.5854666666667, 353.9254666666667, 280.58279999999996], [358.956, 359.21200000000005, 359.21200000000005, 306.2504, 359.21200000000005], [331.918, 422.43800000000005, 331.918, 359.21200000000005, 331.918], [357.5856, 357.5856, 357.5856, 357.5856, 382.8269333333334], [383.2532, 400.34253333333334, 383.2532, 400.34253333333334, 400.34253333333334], [408.92080000000004, 408.92080000000004, 408.92080000000004, 408.92080000000004, 408.92080000000004], [434.58840000000004, 451.6777333333334, 434.58840000000004, 434.5884000000001, 583.0817333333331]]

mas[14] = [[284.24133333333333, 290.8946666666667, 284.02799999999996, 290.54346666666663, 241.50346666666664], [291.068, 291.28133333333335, 291.28133333333335, 261.8362666666667, 291.068], [291.28133333333335, 282.41973333333334, 298.2746666666667, 282.41973333333334, 291.28133333333335], [291.28133333333335, 291.28133333333335, 291.28133333333335, 298.2746666666667, 291.28133333333335], [306.49733333333336, 306.49733333333336, 306.49733333333336, 323.5866666666667, 306.49733333333336], [327.0808, 334.07413333333335, 327.0808, 344.17013333333335, 328.7208000000001], [364.7536, 347.66426666666666, 354.6576, 347.66426666666666, 347.66426666666666], [368.2477333333334, 368.2477333333334, 385.3370666666667, 385.3370666666667, 368.2477333333334], [405.9205333333334, 388.8312000000001, 388.8312000000001, 388.8312000000001, 405.9205333333334]]

mas[15] = [[242.792, 242.792, 316.9352, 316.8418666666667, 316.8418666666667], [335.4906666666667, 335.4906666666667, 335.15066666666667, 335.3173333333334, 335.3173333333334], [276.41759999999994, 276.41759999999994, 276.41759999999994, 345.072, 422.6440000000001], [300.69680000000005, 300.69680000000005, 300.69680000000005, 300.69680000000005, 300.69680000000005], [345.32800000000003, 324.976, 324.976, 345.32800000000003, 324.976], [349.25520000000006, 374.7365333333334, 349.25520000000006, 349.25520000000006, 349.25520000000006], [373.5344, 373.5344, 390.62373333333335, 390.62373333333335, 373.5344], [397.8136000000001, 397.8136000000001, 414.9029333333334, 414.9029333333334, 397.81360000000006], [498.62613333333337, 439.18213333333335, 439.18213333333335, 447.5741333333334, 422.0928]]

mas[16] = [[241.47933333333333, 249.77533333333335, 283.78666666666663, 290.65000000000003, 249.77533333333335], [291.04, 261.788, 261.788, 323.076, 261.788], [291.04, 282.3473333333333, 291.04, 291.04, 291.04], [291.04, 291.04, 291.04, 291.04, 291.04], [306.37666666666667, 323.466, 313.37000000000006, 306.37666666666667, 384.754], [326.936, 344.0253333333333, 344.0253333333333, 326.936, 326.936], [347.4953333333334, 347.4953333333334, 364.58466666666675, 347.4953333333334, 364.58466666666675], [385.14400000000006, 368.0546666666667, 385.14400000000006, 368.0546666666667, 385.14400000000006], [388.61400000000003, 388.61400000000003, 395.6073333333334, 388.61400000000003, 388.61400000000003]]


# temp = [ ael + bel for (ael, bel) in zip(a,b)]
# temp = list(zip(*mas))

def sum_list(m):
    result = []
    for el in m:
        result += el
    return result

temp = [sum_list(m) for m in zip(*mas)]

avr_results = [sum(t)/len(t) for t in temp]
sigma_results =[math.sqrt(sum((el - avr)*(el - avr) for el in t)/len(t)) for (t, avr) in zip(temp, avr_results)]
max_min_results = [(max(t), min(t)) for t in temp]
results = [(avr, sigma, (sigma/avr)*100, max, min) for (avr, sigma, (max, min)) in zip(avr_results, sigma_results, max_min_results)]
print(results)

wf_added_times = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]
nominal_gaheft = 219.82133333333329
nominal_heft = 311.89333333333343

gaheft_times = [nominal_gaheft * time for time in wf_added_times]
heft_times = [nominal_heft * time for time in wf_added_times]
print("gaheft_times: " + str(gaheft_times))
print("heft_times: " + str(heft_times))