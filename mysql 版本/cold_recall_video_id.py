import pymysql
import os
import sys
import datetime
import random

from setting import HOST, PORT, USER, PASSWORD, DATABASE_1, DATABASE_2, NEW_VIDEO_TIME

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(BASE_DIR))


class Cold_recall(object):

    def __init__(self):
        # 连接mysql数据库rcmd，定时更新总表到reco库all_video表
        self.conn1 = pymysql.connect(host=HOST,
                                     user=USER,
                                     port=PORT,
                                     password=PASSWORD,
                                     database=DATABASE_1)

        # 连接mysql数据库reco（冷启动的推荐）
        self.conn2 = pymysql.connect(host=HOST,
                                     user=USER,
                                     port=PORT,
                                     password=PASSWORD,
                                     database=DATABASE_2)

    def update_all_video(self):
        """
        定时更新mysql数据库rcmd全部视频id至reco库all_video表
        :return:
        """
        # 游标1操作数据库rcmd, 读取全部视频的id
        cursor1 = self.conn1.cursor()
        # 游标2操作数据库reco, 写入视频id到all_video_2
        cursor2 = self.conn2.cursor()

        cursor1.execute("select distinct vid from user_profile01 ")
        data = cursor1.fetchall()
        data = list(data)   #转换成列表，便于随机打散

        for line in data:
            cursor2.execute('insert into all_video_2(video_id) values(%s);', line)


        # 提交事务
        self.conn2.commit()
        # 关闭游标和数据库的连接
        cursor1.close()
        cursor2.close()
        self.conn1.close()
        self.conn2.close()


    def recall_video_id_1(self):
        """
        方案1: 前期只进行随机推荐
        :return:
        """
        # 游标1读取总视频id，从中随机打散选取100个视频id
        cursor1 = self.conn2.cursor()
        # 游标2负责将100个视频id写入到rand_recommend
        cursor2 = self.conn2.cursor()

        # 1、读取随机视频100个
        sql_1 = "select * from all_video_2"
        self.conn2.ping(reconnect=True)
        cursor1.execute(sql_1)
        random_data = cursor1.fetchall()
        random_data = list(random_data)  # 转换成列表，便于随机打散
        random.shuffle(random_data)

        for line in random_data[:500]:
            cursor2.execute('insert into cold_recall(video_id) values(%s);', line)

        # 提交事务
        self.conn2.commit()
        # 关闭游标和数据库的连接
        cursor1.close()
        cursor2.close()
        self.conn2.close()


    # def scheduler(self, m=23, ):
    #     """
    #     每天更新一次总表
    #     :return:
    #     """



    def recall_video_id_2(self):
        """
        方案2：后期有新视频、热门视频，连同随机推荐，三路一起召回
        连接三个表new_video、hot_video, random_recommend
        从三个表按规则读取数据汇总到cold_recall
        :return:
        """
        # 游标1读取热门视频hot_video，从中选取50个新视频
        cursor1 = self.conn2.cursor()
        # 游标2读取new_video，从中选取30个热门视频
        cursor2 = self.conn2.cursor()
        # 游标3读取random_recommend, 从中选取20个随机推荐视频
        cursor3 = self.conn2.cursor()
        # 游标4负责将三路召回的100个视频id写入random_recommend
        cursor4 = self.conn2.cursor()

        # 1、读取热门视频50个
        # 从热门视频表里读取100个
        sql_1 = "select * from hot_video order by score limit 100"
        cursor1.execute(sql_1)
        hot_data = cursor1.fetchall()
        # 在热度score排名100的视频中随机打散，选取50个
        hot_data = list(hot_data)   #转换成列表，便于随机打散
        random.shuffle(hot_data)
        for line in hot_data[:50]:
            cursor4.execute('insert into cold_recall(video_id) values(%s);', line)

        # 2、读取新视频30个
        # 读取全部的新视频,随机
        sql_2 = "select * from new_video"
        cursor2.execute(sql_2)
        new_data = cursor2.fetchall()
        # 全部新视频随机打散，选取30个
        new_data = list(new_data)    #转换成列表，便于随机打散
        random.shuffle(new_data)
        for line in new_data[:30]:
            cursor4.execute('insert into cold_recall(video_id) values(%s);', line)

        # 3、读取随机视频20个
        sql_3 = "select * from all_video"
        cursor3.execute(sql_3)
        random_data = cursor3.fetchall()
        random_data = list(random_data)    # 转换成列表，便于随机打散
        random.shuffle(random_data)

        for line in random_data[:20]:
            cursor4.execute('insert into cold_recall(video_id) values(%s);', line)

        # 提交事务
        self.conn2.commit()
        # 关闭游标和数据库的连接
        cursor1.close()
        cursor2.close()
        cursor3.close()
        cursor4.close()
        self.conn2.close()



if __name__ == '__main__':
    ore = Cold_recall()
    ore.update_all_video()
    # 前期采用方案1，只进行随机推荐冷启动
    ore.recall_video_id_1()
    # 后期采用方案2，进行热门视频、新视频、随机推荐三路召回
    # ore.recall_video_id_2()





