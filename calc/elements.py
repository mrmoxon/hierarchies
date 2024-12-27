from typing import List, Tuple, Dict
from collections import defaultdict
import sys

class PeriodicSpeller:
    def __init__(self):
        # Previous element definitions remain the same
        self.elements = {
            'H', 'He', 'Li', 'Be', 'B', 'C', 'N', 'O', 'F', 'Ne',
            'Na', 'Mg', 'Al', 'Si', 'P', 'S', 'Cl', 'Ar', 'K', 'Ca',
            'Sc', 'Ti', 'V', 'Cr', 'Mn', 'Fe', 'Co', 'Ni', 'Cu', 'Zn',
            'Ga', 'Ge', 'As', 'Se', 'Br', 'Kr', 'Rb', 'Sr', 'Y', 'Zr',
            'Nb', 'Mo', 'Tc', 'Ru', 'Rh', 'Pd', 'Ag', 'Cd', 'In', 'Sn',
            'Sb', 'Te', 'I', 'Xe', 'Cs', 'Ba', 'La', 'Ce', 'Pr', 'Nd',
            'Pm', 'Sm', 'Eu', 'Gd', 'Tb', 'Dy', 'Ho', 'Er', 'Tm', 'Yb',
            'Lu', 'Hf', 'Ta', 'W', 'Re', 'Os', 'Ir', 'Pt', 'Au', 'Hg',
            'Tl', 'Pb', 'Bi', 'Po', 'At', 'Rn', 'Fr', 'Ra', 'Ac', 'Th',
            'Pa', 'U', 'Np', 'Pu', 'Am', 'Cm', 'Bk', 'Cf', 'Es', 'Fm',
            'Md', 'No', 'Lr', 'Rf', 'Db', 'Sg', 'Bh', 'Hs', 'Mt', 'Ds',
            'Rg', 'Cn', 'Nh', 'Fl', 'Mc', 'Lv', 'Ts', 'Og'
        }
        self.elements_lower = {elem.lower() for elem in self.elements}

    def find_missing_letters(self, word: str, solution: List[str]) -> List[str]:
        """Find which letters are missing from a partial solution."""
        word = word.lower()
        # Create a string of all letters covered by the solution
        covered_letters = ''
        for element in solution:
            covered_letters += element.lower()
        
        # Create copy of word to track missing letters
        word_remaining = list(word)
        covered_remaining = list(covered_letters)
        
        # Remove matched letters
        for letter in covered_letters:
            if letter in word_remaining:
                word_remaining.remove(letter)
                covered_remaining.remove(letter)
        
        # Return remaining letters from original word
        return [letter.upper() for letter in word_remaining]

    # Previous methods remain the same
    def find_all_spellings(self, word: str) -> List[List[str]]:
        word = word.lower()
        
        def backtrack(remaining: str, current_path: List[str], all_paths: List[List[str]]):
            if not remaining:
                all_paths.append(current_path[:])
                return
            
            for length in [1, 2]:
                if len(remaining) >= length:
                    prefix = remaining[:length]
                    if prefix in self.elements_lower:
                        original_case = next(e for e in self.elements if e.lower() == prefix)
                        backtrack(remaining[length:], current_path + [original_case], all_paths)
        
        solutions = []
        backtrack(word, [], solutions)
        return solutions
    
    def find_closest_match(self, word: str) -> List[Tuple[List[str], int, List[str]]]:
        """Modified to include missing letters in the return value."""
        word = word.lower()
        best_solutions = []
        min_missing = float('inf')
        
        def try_partial_match(pos: int, current_path: List[str], letters_used: int):
            nonlocal min_missing, best_solutions
            missing_letters = len(word) - letters_used
            
            if pos >= len(word):
                if missing_letters <= min_missing:
                    if missing_letters < min_missing:
                        best_solutions = []
                        min_missing = missing_letters
                    missing_letters_list = self.find_missing_letters(word, current_path)
                    best_solutions.append((current_path, missing_letters, missing_letters_list))
                return
            
            for length in [1, 2]:
                if pos + length <= len(word):
                    prefix = word[pos:pos + length]
                    if prefix in self.elements_lower:
                        original_case = next(e for e in self.elements if e.lower() == prefix)
                        try_partial_match(pos + length, current_path + [original_case], 
                                       letters_used + length)
            
            try_partial_match(pos + 1, current_path, letters_used)
        
        try_partial_match(0, [], 0)
        return best_solutions
    
    def spell_word(self, word: str) -> Dict:
        exact_solutions = self.find_all_spellings(word)
        closest_matches = self.find_closest_match(word)
        
        return {
            'word': word,
            'can_be_spelled': len(exact_solutions) > 0,
            'exact_solutions': exact_solutions,
            'closest_matches': closest_matches if not exact_solutions else []
        }

def process_file(filename: str, output_filename: str = None, missing_letters_filename: str = None):
    """
    Process a file containing words, one per line.
    Now includes option for a separate missing letters summary file.
    """
    speller = PeriodicSpeller()
    results = []
    missing_letters_summary = []
    
    try:
        # Read input file
        with open(filename, 'r', encoding='utf-8') as f:
            words = [line.strip() for line in f if line.strip()]
        
        # Process each word
        for word in words:
            result = speller.spell_word(word)
            results.append(result)
            
            # If word can't be spelled exactly, add its missing letters to summary
            if not result['can_be_spelled'] and result['closest_matches']:
                # Get the first (best) match's missing letters
                best_match = result['closest_matches'][0]
                missing_letters = best_match[2]  # [2] contains missing letters list
                if missing_letters:
                    missing_letters_summary.append(f"{word}: {', '.join(missing_letters)}")
            
            # Console output remains the same
            print(f"\nAnalyzing: {result['word']}")
            if result['can_be_spelled']:
                print("✓ Can be spelled with elements!")
                print("Solutions:")
                for i, solution in enumerate(result['exact_solutions'], 1):
                    print(f"{i}. {' + '.join(solution)}")
            else:
                print("✗ Cannot be spelled exactly with elements")
                print("Closest matches:")
                for solution, missing, missing_letters in result['closest_matches']:
                    print(f"Missing {missing} letters: {' + '.join(solution)}")
                    if missing_letters:
                        print(f"Letters needed: {', '.join(missing_letters)}")
        
        # Write main output file if specified
        if output_filename:
            with open(output_filename, 'w', encoding='utf-8') as f:
                for result in results:
                    f.write(f"\nWord: {result['word']}\n")
                    if result['can_be_spelled']:
                        f.write("Can be spelled with elements!\n")
                        f.write("Solutions:\n")
                        for i, solution in enumerate(result['exact_solutions'], 1):
                            f.write(f"{i}. {' + '.join(solution)}\n")
                    else:
                        f.write("Cannot be spelled exactly with elements\n")
                        f.write("Closest matches:\n")
                        for solution, missing, missing_letters in result['closest_matches']:
                            f.write(f"Missing {missing} letters: {' + '.join(solution)}\n")
                            if missing_letters:
                                f.write(f"Letters needed: {', '.join(missing_letters)}\n")
            print(f"\nResults have been saved to {output_filename}")
        
        # Write missing letters summary file if specified
        if missing_letters_filename and missing_letters_summary:
            with open(missing_letters_filename, 'w', encoding='utf-8') as f:
                for line in missing_letters_summary:
                    f.write(f"{line}\n")
            print(f"Missing letters summary saved to {missing_letters_filename}")
                
    except FileNotFoundError:
        print(f"Error: Could not find file '{filename}'")
    except Exception as e:
        print(f"Error processing file: {str(e)}")

def main():
    if len(sys.argv) < 2:
        print("Usage: python script.py input_file.txt output_file.txt missing_letters_file.txt")
        return
    
    input_file = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else None
    missing_letters_file = sys.argv[3] if len(sys.argv) > 3 else None
    process_file(input_file, output_file, missing_letters_file)

if __name__ == "__main__":
    main()