from collections import defaultdict
from typing import Dict, List, Set

def analyze_single_missing_letters(data: List[str]) -> Dict[str, List[str]]:
    """
    Group countries by their single missing letter.
    Only includes countries that are missing exactly one letter.
    """
    # Dictionary to store letter -> countries mapping
    letter_countries = defaultdict(list)
    
    for line in data:
        if ':' not in line:
            continue
            
        country, letters = line.split(':')
        country = country.strip()
        # Split letters by comma and clean up
        missing_letters = [l.strip() for l in letters.split(',')]
        
        # Only process countries with exactly one missing letter
        if len(missing_letters) == 1:
            letter = missing_letters[0]
            letter_countries[letter].append(country)
    
    return letter_countries

def format_analysis(letter_countries: Dict[str, List[str]]) -> str:
    """
    Format the analysis results into a readable report.
    """
    # Sort letters by number of countries (descending)
    sorted_letters = sorted(letter_countries.items(), 
                          key=lambda x: len(x[1]), 
                          reverse=True)
    
    output = []
    for letter, countries in sorted_letters:
        # Create the summary line
        summary = f"+{letter} = +{len(countries)} countries:"
        # Add the list of countries
        countries_list = ", ".join(countries)
        output.append(f"{summary}\n{countries_list}\n")
    
    return "\n".join(output)

def main():
    # Example data
    data = """Albania: A
Andorra: R, A
Armenia: M, E, A
Austria: T, R, A
Azerbaijan: Z, A, J, A
Belgium: L, G, M
Bosnia and Herzegovina: Z, E, G, A, A
Bulgaria: L, R, A
Croatia: A
Czech: Z, E
Denmark: D, E, M
Estonia: T, A
Georgia: A
Germany: R, M, A
Greece: G, E
Hungary: R
Hungary: G
Italy: L
Italy: T
Kazakhstan: Z, A, A
Latvia: T, A
Liechtenstein: E
Lithuania: A
Luxembourg: M
Malta: M
Moldova: L, D, A
Montenegro: G, R
North Macedonia: R, M, E, D, A
Norway: R, A
Portugal: R, T, L
Romania: R, M, A, A
Russia: A
San Marino: M, A
Serbia: A
Slovakia: L, A, A
Slovenia: L, E, A
Sweden: E, D, E
Switzerland: T, Z
Turkey: T, R, E
United Kingdom: D, M
England: E, G
Scotland: A
Scotland: T
Northern Ireland: R
Vatican City: T"""

    # Process the data
    letter_countries = analyze_single_missing_letters(data.split('\n'))
    
    # Generate and print report
    print(format_analysis(letter_countries))

if __name__ == "__main__":
    main()