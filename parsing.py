import os
import threading

import pandas as pd


# Класс для вычисления среднего значения писем пользователей.
class AvgCountMsg:
    # По умолчанию количество потоков, это количество ядер умноженое на 5.
    def __init__(self, path_xl='logs_sample.xlsx',
                 threads_count=os.cpu_count()*5):
        # Лист excel файла.
        self.sheet_xl = pd.DataFrame()
        # Атрибут для колонки cid.
        self.cids_arr = []
        # Путь excel файла.
        self.path_xl = path_xl
        # Количество всех писем пользователей, где писем больше 1.
        self.all_msg_count = 0
        # Количество всех пользователей, где писем больше 1.
        self.all_email_count = 0
        # Количество потоков.
        self.threads_count = threads_count
        # Среднее значение писем на пользователя, где писем больше 1.
        self.avg = 0

    # Метод для чтения excel файла.
    def read_file_xl(self):
        # Чтение файла при  помощи pandas.
        xl = pd.ExcelFile(self.path_xl)
        # Выбор Листа.
        self.sheet_xl = xl.parse(xl.sheet_names[0])

    # Метод для разбиения данных столбца cid.
    def split_list(self):
        sheet_xl_len = len(self.sheet_xl)
        # Примерное число строк для разбиения.
        div = sheet_xl_len // self.threads_count
        # Начальная позиция массива.
        bgn = 0
        # Конечная позиция для массива.
        end = div
        # Пока конечная позиция не достигнит конца.
        while sheet_xl_len > end:
            # Якорь.
            temp = end - 1
            # Выполнять цикл пока данное значение не будет равняться следующиму.
            while self.sheet_xl['cid'][temp] == self.sheet_xl['cid'][end]:
                end += 1
            # Добвать в массив часть данных из cid.
            self.cids_arr.append(self.sheet_xl['cid'][bgn: end])
            # Конец становится началом.
            bgn = end
            # Начало двигается на след. часть.
            end += div
        # Добавляем оставшиюся часть в массив.
        self.cids_arr.append(self.sheet_xl['cid'][bgn: sheet_xl_len])

    # Метод для счета количества пользователей и писем.
    def add_email_and_mgs_counts(self, cids):
        # Якорь для сравнения следующих пользователей.
        email_temp = str()
        # Переменная для подсчета писем на одного пользователя.
        msg_count_temp = 1
        # Перебираем письма.
        for email in cids:
            # Если один пользователь идет подряд.
            if email == email_temp:
                # Прибавить 1 к количеству писем у пользователя.
                msg_count_temp += 1
            else:
                # Если у пользователя писем больше 1.
                if msg_count_temp > 1:
                    # Добавить к общему количеству писем.
                    self.all_msg_count += msg_count_temp
                    # Добавить к общему количеству пользователей.
                    self.all_email_count += 1
                # Сменить якорь на текущего пользоватлея.
                email_temp = email
                # Сбросить количество писем.
                msg_count_temp = 1
        # Проверить для последнего пользователя.
        if msg_count_temp > 1:
            self.all_msg_count += msg_count_temp
            self.all_email_count += 1

    # Метод для созднания и запуска потоков.
    def threads_start(self):
        # Массив для всех потоков.
        threads_arr = []
        for i in range(self.threads_count):
            # Создаем поток.
            thread_temp = threading.Thread(
                target=self.add_email_and_mgs_counts,
                args=(self.cids_arr[i],)
            )
            # Добавляем поток в массив потоков.
            threads_arr.append(thread_temp)
            # Запускаем данный поток.
            thread_temp.start()

        # Перебираем все потоки
        for thread_temp in threads_arr:
            # Ждем пока все потоки не выполнятся.
            thread_temp.join()

    # Метод для нахождения среднего значения.
    def set_avg(self):
        # Читаем excel файл.
        self.read_file_xl()
        # Сортируем столбец cid для быстрого поиска одинаковых email.
        self.sheet_xl['cid'] = sorted(self.sheet_xl['cid'])
        # Разбиваем данные cid для потоков.
        self.split_list()
        # Создаем и запускаем потоки передав данные cid.
        self.threads_start()
        # Вычисляем среднее значение.
        self.avg = self.all_msg_count / self.all_email_count

    # Метод для получения среднего значения.
    def get_avg(self):
        return self.avg


if __name__ == '__main__':
    avg_count_msg = AvgCountMsg()
    avg_count_msg.set_avg()
    print(avg_count_msg.get_avg())
