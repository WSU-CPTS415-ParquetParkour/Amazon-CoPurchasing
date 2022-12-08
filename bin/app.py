#! /usr/bin/python3

import os
import sys
import re
import configparser as cfg
import decimal
from PyQt5 import uic, QtCore, QtWidgets
from PyQt5.QtCore import QRect, QCoreApplication, QMetaObject
from PyQt5.QtWidgets import (QMainWindow, QApplication, QWidget, QAction,
                            QDialog, QMessageBox, QTableWidget, QTableWidgetItem,
                            QVBoxLayout, QPushButton, QLabel, QRadioButton, QTextEdit,
                            QMenuBar, QMenu, QStatusBar)
from PyQt5.QtGui import QIcon, QPixmap, QTextCharFormat, QFont

project_root = re.sub('(?<=Amazon-CoPurchasing).*', '', os.path.abspath('.'))
config_path = os.path.join(project_root, 'etc', 'config.ini')

# Add reference path to access files in /lib/ (JR)
sys.path.insert(0, os.path.join(project_root, 'lib'))

from acpN4J import N4J

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

    def setupUi(self, MainWindow):
        if not MainWindow.objectName():
            MainWindow.setObjectName(u"MainWindow")
        MainWindow.resize(1039, 805)
        self.centralwidget = QWidget(MainWindow)
        self.centralwidget.setObjectName(u"centralwidget")
        self.radioButton = QRadioButton(self.centralwidget)
        self.radioButton.setObjectName(u"radioButton")
        self.radioButton.setGeometry(QRect(340, 230, 89, 20))
        self.radioButton_2 = QRadioButton(self.centralwidget)
        self.radioButton_2.setObjectName(u"radioButton_2")
        self.radioButton_2.setGeometry(QRect(390, 230, 89, 20))
        self.radioButton_3 = QRadioButton(self.centralwidget)
        self.radioButton_3.setObjectName(u"radioButton_3")
        self.radioButton_3.setGeometry(QRect(430, 230, 89, 20))
        self.radioButton_4 = QRadioButton(self.centralwidget)
        self.radioButton_4.setObjectName(u"radioButton_4")
        self.radioButton_4.setGeometry(QRect(300, 230, 89, 20))
        self.radioButton_5 = QRadioButton(self.centralwidget)
        self.radioButton_5.setObjectName(u"radioButton_5")
        self.radioButton_5.setGeometry(QRect(480, 230, 89, 20))
        self.textEdit = QTextEdit(self.centralwidget)
        self.textEdit.setObjectName(u"textEdit")
        self.textEdit.setGeometry(QRect(300, 150, 241, 71))
        self.textEdit_2 = QTextEdit(self.centralwidget)
        self.textEdit_2.setObjectName(u"textEdit_2")
        self.textEdit_2.setGeometry(QRect(300, 260, 241, 41))
        self.label = QLabel(self.centralwidget)
        self.label.setObjectName(u"label")
        self.label.setGeometry(QRect(300, 130, 111, 16))
        self.label_2 = QLabel(self.centralwidget)
        self.label_2.setObjectName(u"label_2")
        self.label_2.setGeometry(QRect(560, 230, 81, 16))
        self.pushButton = QPushButton(self.centralwidget)
        self.pushButton.setObjectName(u"pushButton")
        self.pushButton.setGeometry(QRect(300, 320, 241, 41))
        self.pushButton.setMouseTracking(False)
        self.textEdit_3 = QTextEdit(self.centralwidget)
        self.textEdit_3.setObjectName(u"textEdit_3")
        self.textEdit_3.setGeometry(QRect(30, 430, 981, 331))
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


if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = AcpApp()
    window.show()

    with open(ui_qss, 'r') as f:
        style = f.read()
        app.setStyleSheet(style)

    sys.exit(app.exec_())
