#! /usr/bin/python3

import os
import sys
import re
import configparser as cfg
import decimal
import json
from PyQt5 import uic, QtCore, QtWidgets
from PyQt5.QtCore import QRect, QCoreApplication, QMetaObject
from PyQt5.QtWidgets import (QMainWindow, QApplication, QWidget, QAction,
                            QDialog, QMessageBox, QTableWidget,QListWidget,QGridLayout, QTableWidgetItem,
                            QVBoxLayout, QPushButton, QLabel, QRadioButton, QTextEdit,
                            QMenuBar, QMenu, QStatusBar)
from PyQt5.QtGui import QIcon, QPixmap, QTextCharFormat, QFont

project_root = re.sub('(?<=Amazon-CoPurchasing).*', '', os.path.abspath('.'))
config_path = os.path.join(project_root, 'etc', 'config.ini')

# Add reference path to access files in /lib/ (JR)
sys.path.insert(0, os.path.join(project_root, 'lib'))

from acpN4J import N4J
category_path = os.path.join(project_root,'etc','node_property_keys.json')
x=open(category_path)
dict_of_cats = json.load(x)
print(dict_of_cats)

config = cfg.ConfigParser()
config.read(config_path)

ui_path = os.path.join(project_root, 'ui')
ui_main_window = os.path.join(ui_path, 'Mockup_UI_query_program.ui')
ui_qss  = os.path.join(project_root, 'etc', 'style.qss')

Ui_MainWindow, QtBaseClass = uic.loadUiType(ui_main_window)

class AcpApp(QMainWindow):
    def __init__(self):
        super(AcpApp, self).__init__()
        self.ui = Ui_MainWindow()
        self.ui.setupUi(self)
        self.loadList()
        self.ui.listWidget.itemClicked.connect(self.Clicked1)
        self.ui.listWidget_2.itemClicked.connect(self.Clicked2)
        self.ui.listWidget_3.itemClicked.connect(self.Clicked3)


    def loadList(self):
                self.ui.listWidget.clear()
                self.ui.listWidget.addItem('PRODUCT')
                self.ui.listWidget.addItem('CATEGORY')
                self.ui.listWidget.addItem('CUSTOMER')
                self.ui.listWidget.addItem('REVIEW')
                
            
                

        #DEMO CODE NOT NECESSARY FOR NOW
                # try:
                #     results = self.executeQuery(sql_str)
                #     #print(results)
                #     for row in results:
                #         self.ui.listWidget.addItem(row[0])
                #         self.ui.stateList_2.addItem(row[0])
                # except:
                #     print("Query failed!")
                # self.ui.listWidget.setCurrentIndex(0)
                # self.ui.listWidget.clearEditText()
                # self.ui.stateList_2.setCurrentIndex(-1)
                # self.ui.stateList_2.clearEditText()


    def setupUi(self, MainWindow):
        if not MainWindow.objectName():
            MainWindow.setObjectName(u"MainWindow")
        MainWindow.resize(1039, 805)
        self.centralwidget = QWidget(MainWindow)
        self.centralwidget.setObjectName(u"centralwidget")
        self.gridLayout = QGridLayout(self.centralwidget)
        self.gridLayout.setObjectName(u"gridLayout")
        self.listWidget = QListWidget(self.centralwidget)
        self.listWidget.setObjectName(u"listWidget")

        self.gridLayout.addWidget(self.listWidget, 1, 0, 1, 4)

        self.pushButton = QPushButton(self.centralwidget)
        self.pushButton.setObjectName(u"pushButton")
        self.pushButton.setMouseTracking(False)

        self.gridLayout.addWidget(self.pushButton, 4, 2, 1, 1)

        self.label_3 = QLabel(self.centralwidget)
        self.label_3.setObjectName(u"label_3")

        self.gridLayout.addWidget(self.label_3, 1, 5, 1, 1)

        self.radioButton = QRadioButton(self.centralwidget)
        self.radioButton.setObjectName(u"radioButton")

        self.gridLayout.addWidget(self.radioButton, 2, 1, 1, 1)

        self.radioButton_5 = QRadioButton(self.centralwidget)
        self.radioButton_5.setObjectName(u"radioButton_5")

        self.gridLayout.addWidget(self.radioButton_5, 2, 4, 1, 1)

        self.radioButton_4 = QRadioButton(self.centralwidget)
        self.radioButton_4.setObjectName(u"radioButton_4")

        self.gridLayout.addWidget(self.radioButton_4, 2, 0, 1, 1)

        self.textEdit = QTextEdit(self.centralwidget)
        self.textEdit.setObjectName(u"textEdit")

        self.gridLayout.addWidget(self.textEdit, 3, 0, 2, 2)

        self.tableWidget = QTableWidget(self.centralwidget)
        self.tableWidget.setObjectName(u"tableWidget")

        self.gridLayout.addWidget(self.tableWidget, 5, 0, 1, 9)

        self.listWidget_2 = QListWidget(self.centralwidget)
        self.listWidget_2.setObjectName(u"listWidget_2")

        self.gridLayout.addWidget(self.listWidget_2, 1, 6, 1, 1)

        self.radioButton_3 = QRadioButton(self.centralwidget)
        self.radioButton_3.setObjectName(u"radioButton_3")

        self.gridLayout.addWidget(self.radioButton_3, 2, 3, 1, 1)

        self.label_2 = QLabel(self.centralwidget)
        self.label_2.setObjectName(u"label_2")

        self.gridLayout.addWidget(self.label_2, 3, 2, 1, 1)

        self.label_4 = QLabel(self.centralwidget)
        self.label_4.setObjectName(u"label_4")

        self.gridLayout.addWidget(self.label_4, 1, 7, 1, 1)

        self.listWidget_3 = QListWidget(self.centralwidget)
        self.listWidget_3.setObjectName(u"listWidget_3")

        self.gridLayout.addWidget(self.listWidget_3, 1, 8, 1, 1)

        self.label = QLabel(self.centralwidget)
        self.label.setObjectName(u"label")

        self.gridLayout.addWidget(self.label, 0, 0, 1, 2)

        self.radioButton_2 = QRadioButton(self.centralwidget)
        self.radioButton_2.setObjectName(u"radioButton_2")

        self.gridLayout.addWidget(self.radioButton_2, 2, 2, 1, 1)

        MainWindow.setCentralWidget(self.centralwidget)
        self.menubar = QMenuBar(MainWindow)
        self.menubar.setObjectName(u"menubar")
        self.menubar.setGeometry(QRect(0, 0, 1039, 22))
        self.menuFile = QMenu(self.menubar)
        self.menuFile.setObjectName(u"menuFile")
        MainWindow.setMenuBar(self.menubar)
        self.statusbar = QStatusBar(MainWindow)
        self.statusbar.setObjectName(u"statusbar")
        MainWindow.setStatusBar(self.statusbar)

        self.menubar.addAction(self.menuFile.menuAction())

        self.retranslateUi(MainWindow)

        QMetaObject.connectSlotsByName(MainWindow)
        
    # setupUi

    def retranslateUi(self, MainWindow):
        MainWindow.setWindowTitle(QCoreApplication.translate("MainWindow", u"MainWindow", None))
        self.radioButton.setText(QCoreApplication.translate("MainWindow", u">=", None))
        self.radioButton_2.setText(QCoreApplication.translate("MainWindow", u"<", None))
        self.radioButton_3.setText(QCoreApplication.translate("MainWindow", u"<=", None))
        self.radioButton_4.setText(QCoreApplication.translate("MainWindow", u">", None))
        self.radioButton_5.setText(QCoreApplication.translate("MainWindow", u"=", None))
        self.label.setText(QCoreApplication.translate("MainWindow", u"Enter a query below", None))
        self.label_2.setText(QCoreApplication.translate("MainWindow", u"Add condition", None))
        self.pushButton.setText(QCoreApplication.translate("MainWindow", u"Go!", None))
        self.menuFile.setTitle(QCoreApplication.translate("MainWindow", u"File", None))
    # retranslateUi

    def Clicked1(self,item):
        listofitems=[]
        self.ui.listWidget_2.clear()
        if (item.text() == 'CUSTOMER'):
            listofitems = [z for x,y in dict_of_cats.items() if x == 'CUSTOMER' for z in y]
        if (item.text() == 'CATEGORY'):
            listofitems = [z for x,y in dict_of_cats.items() if x == 'CATEGORY' for z in y] 
        if (item.text() == 'PRODUCT'):
            listofitems = [z for x,y in dict_of_cats.items() if x == 'PRODUCT' for z in y]          
        if (item.text() == 'REVIEW'):
            listofitems = [z for x,y in dict_of_cats.items() if x == 'REVIEW' for z in y]          
        
        for it in listofitems:
            self.ui.listWidget_2.addItem(it)


    def Clicked2(self,item):
	    QMessageBox.information(self, "ListWidget", "You clicked: "+item.text())

    # def Clicked3(self,item):
	#     QMessageBox.information(self, "ListWidget", "You clicked: "+item.text())

    #def populate_List_2(self,MainWindow):
        


if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = AcpApp()
    window.show()

    with open(ui_qss, 'r') as f:
        style = f.read()
        app.setStyleSheet(style)

    sys.exit(app.exec_())