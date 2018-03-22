from setuptools import setup

setup(
    name='mike-result-extractor',
    version='0.1.0',
    description='python3 program to push mike11 results to curw db',
    author='thilinamad',
    author_email='madumalt@gmail.com',
    url='https://github.com/CUrW-SL/mike_results_extractor.git',
    dependency_links=[
        "git+https://github.com/gihankarunarathne/CurwMySQLAdapter.git@0.2.3#egg=curwmysqladapter-0.2.3"
    ],
    install_requires=['curwmysqladapter']
)