from setuptools import setup

setup(
    name='landFinder',
    version='0.1',
    packages=['requests', 'bs4', 'pandas', 'sqlalchemy', 're', 'tqdm', 'numpy', 'datetime', 'time', 'random'],
    url='https://github.com/eccoghosp/Landfinder',
    license='MIT License',
    author='Zach Mays',
    author_email='zachary.a.mays@gmail.com',
    description='A web scraper to gather properties from landwatch.com and save it to a SQL server and a .csv'
)
