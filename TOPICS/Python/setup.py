from setuptools import find_packages, setup
from typing import List
def get_requirements() -> List[str]:
    """
    This function returns a list of requirements from the requirements.txt file.
    """
    requirement_lst:List[str]=[]
    try:
        with open('requirements.txt', 'r') as file:
            requirements = file.readlines()
            for line in requirements:
                requirement=line.strip()
                if requirement and requirement != '-e .':
                    requirement_lst.append(requirement)
    except FileNotFoundError:
        print("requirements.txt file not found. Please ensure it exists in the current directory.")
        return []
            
    return requirement_lst

setup(
    name='my_package',
    version='0.1.0',
    author='Ranjith Shada',
    packages=find_packages(),
    install_requires=get_requirements(),
    description='A sample Python package')