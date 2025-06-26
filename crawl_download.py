import time
import os
from idlelib.iomenu import encoding

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver import ActionChains
from selenium.webdriver.common.action_chains import ActionBuilder
from selenium.webdriver.edge.service import Service as EdgeService
import subprocess
from selenium.webdriver.common.keys import Keys
from urllib.request import urlretrieve
import cv2 as cv
import random
import requests
import json
import cv2
import io
import base64
import numpy as np
from PIL import Image


def image_to_base64(image_path):
    """
        将图片转为 Base64流
    :param image_path: 图片路径
    :return:
    """
    with open(image_path, "rb") as file:
        base64_data = base64.b64encode(file.read())  # base64编码
    return base64_data


def get_distance(bg_img_path, slider_img_path):
    url = "https://api.decodecaptcha.com/images?key=40325a6ccde112e4a9219fd71216b180&image_id=3160101"

    payload = json.dumps({
        "image": str(bg_img_path,encoding='utf-8'),
        "title": str(slider_img_path,encoding='utf-8'),
    })
    headers = {
        'Content-Type': 'application/json'
    }

    response = requests.request("POST", url, headers=headers, data=payload,verify=False)
    print("请求结果：")
    print(response.text)
    print("end")
    # 解析 JSON
    data = json.loads(response.text)

    # 提取并解析 "data" 字段
    data_field = data["data"]
    # 提取 竖线后面的数字
    value = data_field.split("|")[1]
    print(value)  # 输出：222
    return int(value)



def get_tracks(distance):
    '''滑动轨迹'''
    tracks = []
    v = 0
    t = 0.8  # 单位时间
    current = 0  # 滑块当前位移
    #distance += 4  # 多移动10px, 然后回退
    while current < distance:
        if current < distance * 5 / 8:
            a = random.randint(1, 3)
        else:
            a = -random.randint(2, 4)
        v0 = v  # 初速度
        track = v0 * t + 0.5 * a * (t ** 2)  # 单位时间（0.2s）的滑动距离
        tracks.append(round(track))  # 加入轨迹
        current += round(track)
        v = v0 + a * t
    # 回退到大致位置
    # for i in range(5):
    #     tracks.append(-random.randint(1, 3))
    return tracks

def mouse_move(driver,slide,tracks):
    '''鼠标滑动'''
    # 鼠标点击滑块并按照不放
    ActionChains(driver).click_and_hold(slide).perform()
    # 按照轨迹进行滑动，
    for track in tracks:
        ActionChains(driver).move_by_offset(track, 0).perform()
    ActionChains(driver).release(slide).perform()


def click_cancle(dr,xpath):
    try:
        # 点击取消
        cancel = dr.find_element(By.XPATH, xpath)
        dr.execute_script("arguments[0].click();", cancel)

    except Exception as e:
        print(e)

def cancle(driver):
    click_cancle(driver,'/html/body/div[2]/div[2]/div[2]/div[3]/ul[1]/li[2]/i')
    click_cancle(driver,'/html/body/div[2]/div[2]/div[2]/div[3]/ul[1]/li[3]/i')
    click_cancle(driver,'/html/body/div[2]/div[2]/div[2]/div[3]/ul[1]/li[4]/i')
    click_cancle(driver,'/html/body/div[2]/div[2]/div[2]/div[3]/ul[1]/li[7]/i')
    click_cancle(driver,'/html/body/div[2]/div[2]/div[2]/div[3]/ul[1]/li[8]/i')
    click_cancle(driver,'/html/body/div[2]/div[2]/div[2]/div[3]/ul[1]/li[9]/i')
    click_cancle(driver,'/html/body/div[2]/div[2]/div[2]/div[3]/ul[1]/li[10]/i')


from selenium import webdriver


def create_edge_driver(*, headless=False):
    # 配置浏览器选项
    options = webdriver.EdgeOptions()

    # 启用无头模式（可选）
    if headless:
        options.add_argument('--headless')

    # 隐藏自动化特征
    #options.add_experimental_option('excludeSwitches', ['enable-automation'])  # 移除自动化控制提示
    #options.add_experimental_option('useAutomationExtension', False)  # 禁用自动化扩展
    # opt.set_headless()                            #无窗口模式
    # profile.default_content_setting.popups':0 设置为0表示禁止弹出下载窗口
    # 'download.default_directory':"E:\\dir" 修改下载地址为path
    prefs = {'profile.default_content_setting.popups': 0,
             'download.default_directory': path}
    # custom_user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36 Edg/135.0.0.0"
    # opt.add_argument(f'--user-agent={custom_user_agent}')
    #options.add_experimental_option('prefs', prefs)
    options.add_experimental_option("debuggerAddress", "127.0.0.1:9222")
    # 初始化浏览器驱动
    browser = webdriver.Edge(options=options)
    print(browser.title)
    # 通过CDP命令修改navigator.webdriver属性
    # browser.execute_cdp_cmd(
    #     'Page.addScriptToEvaluateOnNewDocument',
    #     {'source': 'Object.defineProperty(navigator, "webdriver", {get: () => false})'}
    # )
    return browser
def verfiy_slider(driver):
    current_handle = driver.current_window_handle
    # 获取当前所有窗口
    windows = driver.window_handles
    driver.switch_to.window(windows[-1])
    time.sleep(1)
    if driver.title == "拼图校验-中国知网":
        slider_button = driver.find_element(By.ID, 'aliyunCaptcha-sliding-slider')
        bg_img_url = driver.find_element(By.ID, 'aliyunCaptcha-img').get_attribute('src')
        slider_img_url = driver.find_element(By.ID, 'aliyunCaptcha-puzzle').get_attribute('src')
        # bg_img_base64 = bg_img_url[22:]
        # slider_img_base64 = slider_img_url[22:]
        urlretrieve(bg_img_url, bg_img_path)
        urlretrieve(slider_img_url, slider_img_path)
        bg_img_base64=image_to_base64(bg_img_path)
        slider_img_base64=image_to_base64(slider_img_path)
        distance = get_distance(bg_img_base64, slider_img_base64)
        distance += 20
        tracks = get_tracks(distance)
        mouse_move(driver, slider_button, tracks)
        time.sleep(1)
        driver.close()
        driver.switch_to.window(current_handle)
    else:
        print('无拼图验证')
        driver.switch_to.window(current_handle)
def download(start,key, page):
    # browser_path = "C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe"
    # subprocess.Popen([browser_path, '--remote-debugging-port=9222'])
    driver = create_edge_driver(headless=False)
    time.sleep(1)
    driver.get("https://www.cnki.net/")  # 打开网页
    #driver.maximize_window()                      #最大化窗口
    time.sleep(1)  # 加载等待
    # txt_SearchText 为搜索框的位置  键入搜索内容
    driver.find_element(By.ID,'txt_SearchText').send_keys(key)
    # 只筛选期刊
    cancle(driver)
    # 定位到搜索按钮的位置并点击
    action = ActionChains(driver)
    action.move_to_element(driver.find_element(By.XPATH, '/html/body/div[2]/div[2]/div[2]/div[1]/div/div[1]/i')).perform()
    search = driver.find_element(By.XPATH, '/html/body/div[2]/div[2]/div[2]/div[1]/div/div[2]/ul[3]/li[2]/a')
    driver.execute_script("arguments[0].click();", search)
    time.sleep(2)
    ActionBuilder(driver).clear_actions()
    # 定位到搜索按钮的位置并点击
    search = driver.find_element(By.XPATH,'/html/body/div[2]/div[2]/div[2]/div[1]/input[2]')
    driver.execute_script("arguments[0].click();", search)
    time.sleep(2) #等待加载
    # 获取初始所有窗口
    windows_init = driver.window_handles
    # 切换到下载页面对应的窗口
    driver.switch_to.window(windows_init[0])
    print(driver.title)
    try:
        # 点击下载
        # 第一页
        # for i in range(1,21):
        #     search = '/html/body/div[2]/div[2]/div[2]/div[2]/div/div[2]/div/div[1]/div/div/div/table/tbody/tr['+str(i)+']/td[2]/a'
        #     search_click = driver.find_element(By.XPATH,search)
        #     driver.execute_script("arguments[0].click();", search_click)
        #     time.sleep(1)
        #     # 获取当前所有窗口
        #     windows = driver.window_handles
        #     # 切换到下载页面对应的窗口
        #     print(driver.title)
        #     driver.switch_to.window(windows[-1])
        #     print(driver.title)
        #     time.sleep(1)
        #     download_click = driver.find_element(By.ID, 'pdfDown')
        #     driver.execute_script("arguments[0].click();", download_click)
        #
        #     # 滑块验证
        #     verfiy_slider(driver)
        #
        #     time.sleep(1)
        #     driver.close()
        #     time.sleep(1)
        #
        #     #切换到检索目录
        #     driver.switch_to.window(windows_init[0])
        # time.sleep(1)
        # 第二到九页

        for k in range(start,page):
            # page = 'page' + str(k)
            # driver.find_element(By.ID,page).click()
            if k == start:
                for t in range(1, start):
                    ActionChains(driver).send_keys(Keys.RIGHT).perform()
                    ActionBuilder(driver).clear_actions()
                    time.sleep(1)
            else:
                ActionChains(driver).send_keys(Keys.RIGHT).perform()
                ActionBuilder(driver).clear_actions()
                time.sleep(1)
            for i in range(1, 21):
                search = '/html/body/div[2]/div[2]/div[2]/div[2]/div/div[2]/div/div[1]/div/div/div/table/tbody/tr[' + str(i) + ']/td[2]/a'
                search_click = driver.find_element(By.XPATH, search)
                driver.execute_script("arguments[0].click();", search_click)
                time.sleep(1)
                # 获取所有窗口
                windows = driver.window_handles
                # 切换到下载页面对应的窗口
                print(driver.title)
                driver.switch_to.window(windows[-1])
                print(driver.title)
                time.sleep(1)
                download_click = driver.find_element(By.ID, 'pdfDown')
                driver.execute_script("arguments[0].click();", download_click)

                # 滑块验证
                verfiy_slider(driver)

                time.sleep(1)
                driver.close()
                time.sleep(1)
                # 切换到检索目录
                driver.switch_to.window(windows_init[0])
            time.sleep(1)
    except Exception as e:
        print(e)
    time.sleep(1)
    driver.close()

if __name__ == '__main__':
    kw = input("请输入要搜索的关键词：")
    start = int(input("请输入要下载的起始页数："))
    pg = int(input("请输入要下载的页数："))
    path = r'D:\small tool\cnkicrawl'+'\\'+kw
    bg_img_path= 'D:\small tool\\1111\photo\\bg.png'
    slider_img_path= 'D:\small tool\\1111\photo\slider.png'
    time.sleep(2)
    if not os.path.exists(path):
        os.makedirs(path)
    try:
        download(start,kw, pg)
    except Exception as e:
        print(e)
