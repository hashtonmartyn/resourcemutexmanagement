'''
Created on 9/12/2013

@author: henry
'''
from setuptools import setup

setup(name='ResourceMutexManagement',
      version='0.2.0',
      description='Python module for controlling concurrent access to resources',
      author='Henry Ashton-Martyn',
      author_email='henry.ashton.martyn@gmail.com',
      url='http://code.google.com/p/resourcemutexmanagement/',
      license='MIT License',
      py_modules=['ResourceMutexManager'], 
      install_requires=['redis>=2.8.0'],
     )