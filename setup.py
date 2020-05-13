from setuptools import setup

setup(
    name='filemerger',
    description='set of utilities to merge sorted files and aggregate items in them',
    author='Ludovica Pannitto',
    author_email='ellepannitto@gmail.com',
    download_url='https://github.com/ellepannitto/FileMerger',
    url='https://ellepannitto.github.io/',
    version='0.1.0',
    license='MIT',
    packages=['filemerger'],
    classifiers=[
        'Development Status :: 3 - Alpha',      # Chose either "3 - Alpha", "4 - Beta" or "5 - Production/Stable" as the current state of your package
        'Intended Audience :: Developers',      # Define that your audience are developers
        'Topic :: Software Development :: Build Tools',
        'License :: OSI Approved :: MIT License',   # Again, pick a license
        'Programming Language :: Python :: 3',      #Specify which pyhton versions that you want to support
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],
    install_requires=[],
)