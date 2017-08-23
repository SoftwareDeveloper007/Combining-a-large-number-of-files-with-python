import os, time, csv, sys, threading
from collections import OrderedDict

def takeSecond(elm):
    return elm[1]


def printProgressBar(iteration, total, prefix='', suffix='', decimals=1, length=100, fill='█'):
    """
    Call in a loop to create terminal progress bar
    @params:
        iteration   - Required  : current iteration (Int)
        total       - Required  : total iterations (Int)
        prefix      - Optional  : prefix string (Str)
        suffix      - Optional  : suffix string (Str)
        decimals    - Optional  : positive number of decimals in percent complete (Int)
        length      - Optional  : character length of bar (Int)
        fill        - Optional  : bar fill character (Str)
    """
    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    filledLength = int(length * iteration // total)
    bar = fill * filledLength + '-' * (length - filledLength)
    print('\r%s |%s| %s%% %s' % (prefix, bar, percent, suffix), end='\r')
    # Print New Line on Complete
    if iteration == total:
        print()


class input_files():
    def __init__(self):
        self.rain_files = []
        self.temp_files = []

        self.rain_data = {}
        self.temp_data = {}

        self.total_rain_cnt = 0
        self.total_temp_cnt = 0
        self.rain_cnt = 0
        self.temp_cnt = 0

        for (path, dirs, files) in os.walk(os.getcwd() + "\\data"):
            if 'rain' in path and len(files) is not 0:
                for file in files:
                    if 'Notes' in file or 'StnDet' in file:
                        continue
                    else:
                        self.rain_files.append({
                            'path': path,
                            'file': file
                        })

            if 'temp' in path and len(files) is not 0:
                for file in files:
                    if 'Notes' in file or 'StnDet' in file:
                        continue
                    else:
                        self.temp_files.append({
                            'path': path,
                            'file': file
                        })

        if self.rain_files is []:
            print('There are no rain files')
        else:
            self.total_rain_cnt = len(self.rain_files)
            print("Found {0} rain files".format(self.total_rain_cnt))

        if self.temp_files is []:
            print('There are no temperature files')
        else:
            self.total_temp_cnt = len(self.temp_files)
            print("Found {0} temp files".format(self.total_temp_cnt))

    def combine_rain(self):
        self.threads_rain = []
        self.max_threads_rain = 1000

        logTxt = "Started to combine all rain data!!!\n"
        print(logTxt)

        # printProgressBar(0, self.total_rain_cnt, prefix='Progress:', suffix='Complete', length=50)

        while self.threads_rain or self.rain_files:
            for thread in self.threads_rain:
                if not thread.is_alive():
                    self.threads_rain.remove(thread)

            while len(self.threads_rain) < self.max_threads_rain and self.rain_files:
                thread = threading.Thread(target=self.combine_rain_thread)
                thread.setDaemon(True)
                thread.start()
                self.threads_rain.append(thread)

        logTxt = "Completed to combine all rain data!!!\n"
        print(logTxt)

    def combine_rain_thread(self):
        elm = self.rain_files.pop()
        path = elm['path']
        file = elm['file']
        full_name = path + "\\" + file
        data = open(full_name, 'r')
        reader = csv.reader(data)

        for i, row in enumerate(reader):
            if i == 0:
                continue
            try:
                if float(row[5]) > float(self.rain_data[row[1]][row[2]][row[3]][row[4]][1]):
                    self.rain_data[row[1]][row[2]][row[3]][row[4]] = [
                        row[0], row[5], row[6], row[7], row[8], row[9]
                    ]
            except:
                if row[1] not in self.rain_data:
                    self.rain_data[row[1]] = {}
                if row[2] not in self.rain_data[row[1]]:
                    self.rain_data[row[1]][row[2]] = {}
                if row[3] not in self.rain_data[row[1]][row[2]]:
                    self.rain_data[row[1]][row[2]][row[3]] = {}
                if row[4] not in self.rain_data[row[1]][row[2]][row[3]]:
                    self.rain_data[row[1]][row[2]][row[3]][row[4]] = [
                        row[0], row[5], row[6], row[7], row[8], row[9]
                    ]

        self.rain_cnt += 1
        # print('Rain data: {0} out of {1}'.format(self.rain_cnt, self.total_rain_cnt))
        if self.rain_cnt % 100 is 0 or self.rain_cnt == self.total_rain_cnt:
            print('Rain data: {0} out of {1}'.format(self.rain_cnt, self.total_rain_cnt))
            # printProgressBar(self.rain_cnt, self.total_rain_cnt, prefix='Progress:', suffix='Complete', length=50)
            # time.sleep(0.1)

    def save_rain(self):
        logTxt = "Saving rain data now!!!\n"
        print(logTxt)

        output = open('Rain.txt', 'w', encoding='utf-8', newline='')
        writer = csv.writer(output)
        writer.writerow([
            'dc', 'Station Number', 'Year', 'Month', 'Day',
            'Precipitation in the 24 hours before 9am(local time) in mm',
            'Quality of precipitation value', 'Number of days of rain within the days of accumulation',
            'Accumulated number of days over which the precipitation was measured', '#'
        ])

        for k1, v1 in OrderedDict(sorted(self.rain_data.items())).items():
            for k2, v2 in OrderedDict(sorted(v1.items())).items():
                for k3, v3 in OrderedDict(sorted(v2.items())).items():
                    for k4, v4 in OrderedDict(sorted(v3.items())).items():
                        writer.writerow([
                            v4[0], k1, k2, k3, k4, v4[1], v4[2], v4[3], v4[4], v4[5]
                        ])

        output.close()
        logTxt = "Saved rain data successfully!!!\n"
        print(logTxt)
        self.rain_data.clear()

    def combine_temp(self):
        self.threads_temp = []
        self.max_threads_temp = 1000

        logTxt = "Started to combine all temp data!!!\n"
        print(logTxt)

        # printProgressBar(0, self.total_temp_cnt, prefix='Progress:', suffix='Complete', length=50)

        while self.threads_temp or self.temp_files:
            for thread in self.threads_temp:
                if not thread.is_alive():
                    self.threads_temp.remove(thread)

            while len(self.threads_temp) < self.max_threads_temp and self.temp_files:
                thread = threading.Thread(target=self.combine_temp_thread)
                thread.setDaemon(True)
                thread.start()
                self.threads_temp.append(thread)

        logTxt = "Completed to combine all temp data!!!\n"
        print(logTxt)

    def combine_temp_thread(self):
        elm = self.temp_files.pop()
        path = elm['path']
        file = elm['file']
        full_name = path + "\\" + file
        data = open(full_name, 'r')
        reader = csv.reader(data)

        for i, row in enumerate(reader):
            if i == 0:
                continue
            try:
                if float(row[5]) > float(self.temp_data[row[1]][row[2]][row[3]][row[4]][1]):
                    self.temp_data[row[1]][row[2]][row[3]][row[4]] = [
                        row[0], row[5], row[6], row[7], row[8], row[9], row[10], row[11]
                    ]
                if float(row[8]) > float(self.temp_data[row[1]][row[2]][row[3]][row[4]][4]):
                    self.temp_data[row[1]][row[2]][row[3]][row[4]] = [
                        row[0], row[5], row[6], row[7], row[8], row[9], row[10], row[11]
                    ]
            except:
                if row[1] not in self.temp_data:
                    self.temp_data[row[1]] = {}
                if row[2] not in self.temp_data[row[1]]:
                    self.temp_data[row[1]][row[2]] = {}
                if row[3] not in self.temp_data[row[1]][row[2]]:
                    self.temp_data[row[1]][row[2]][row[3]] = {}
                if row[4] not in self.temp_data[row[1]][row[2]][row[3]]:
                    self.temp_data[row[1]][row[2]][row[3]][row[4]] = [
                        row[0], row[5], row[6], row[7], row[8], row[9], row[10], row[11]
                    ]

        self.temp_cnt += 1
        # print('temp data: {0} out of {1}'.format(self.temp_cnt, self.total_temp_cnt))
        if self.temp_cnt % 100 is 0 or self.temp_cnt == self.total_temp_cnt:
            print('temp data: {0} out of {1}'.format(self.temp_cnt, self.total_temp_cnt))
            # printProgressBar(self.temp_cnt, self.total_temp_cnt, prefix='Progress:', suffix='Complete', length=50)
            # time.sleep(0.1)

    def save_temp(self):
        logTxt = "Saving temp data now!!!\n"
        print(logTxt)

        output = open('temp.txt', 'w', encoding='utf-8', newline='')
        writer = csv.writer(output)
        writer.writerow([
            'dc', 'Station Number', 'Year', 'Month', 'Day',
            'Maximum temperature in 24 hours after 9am (local time) in Degrees C',
            'Quality of maximum temperature in 24 hours after 9am (local time)',
            'Days of accumulation of maximum temperature',
            'Minimum temperature in 24 hours before 9am (local time) in Degrees C',
            'Quality of minimum temperature in 24 hours before 9am (local time)',
            'Days of accumulation of minimum temperature',
            '#'
        ])

        for k1, v1 in OrderedDict(sorted(self.temp_data.items())).items():
            for k2, v2 in OrderedDict(sorted(v1.items())).items():
                for k3, v3 in OrderedDict(sorted(v2.items())).items():
                    for k4, v4 in OrderedDict(sorted(v3.items())).items():
                        writer.writerow([
                            v4[0], k1, k2, k3, k4, v4[1], v4[2], v4[3], v4[4], v4[5], v4[6], v4[7]
                        ])

        output.close()
        logTxt = "Saved temp data successfully!!!\n"
        print(logTxt)
        self.temp_data.clear()


if __name__ == '__main__':
    start_time = time.time()

    app = input_files()
    app.combine_rain()
    app.save_rain()
    app.combine_temp()
    app.save_temp()

    elapsed_time = time.time() - start_time
    print('Total elapsed time is {}'.format(elapsed_time))
