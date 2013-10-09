try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

packages = [
    'lee',
    'lee.query',
    'lee.cache'
]

requires = [] # oursql, python3-memcache, sqlite3

setup(
    name='lee',
    version='0.0.2',
    description='The orm framework for mysql and sqlite3',
    author='Li Meng Jun',
    author_email='lmjubuntu@gmail.com',
    url='http://lupino.me',
    packages=packages,
    package_dir={'lee': 'lee'},
    include_package_data=True,
    install_requires=requires,
)
