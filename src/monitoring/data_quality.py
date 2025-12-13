"""
Data quality checks and validation for chess game data.
Ensures data integrity throughout the pipeline.
"""
from typing import Dict, List, Tuple
import re

class DataQualityChecker:
    """Validates chess game data quality and completeness."""
    
    # Valid ECO code pattern (A00-E99)
    ECO_PATTERN = re.compile(r'^[A-E]\d{2}$')
    
    # Valid result patterns
    VALID_RESULTS = {'1-0', '0-1', '1/2-1/2', '*'}
    
    def __init__(self):
        self.stats = {
            'total_checked': 0,
            'valid': 0,
            'invalid': 0,
            'warnings': 0,
            'errors_by_type': {}
        }
    
    def validate_game(self, game_data: Dict) -> Tuple[bool, List[str]]:
        """
        Validate a single game record.
        
        Returns:
            Tuple of (is_valid, list_of_issues)
        """
        self.stats['total_checked'] += 1
        issues = []
        
        # Required fields check
        required_fields = ['white', 'black', 'result', 'moves', 'num_moves']
        for field in required_fields:
            if field not in game_data or game_data[field] is None:
                issues.append(f"Missing required field: {field}")
        
        # Result validation
        if 'result' in game_data:
            if game_data['result'] not in self.VALID_RESULTS:
                issues.append(f"Invalid result: {game_data['result']}")
        
        # ECO code validation
        if 'eco' in game_data and game_data['eco']:
            if not self.ECO_PATTERN.match(game_data['eco']):
                issues.append(f"Invalid ECO code: {game_data['eco']}")
        
        # Moves count validation
        if 'num_moves' in game_data:
            num_moves = game_data['num_moves']
            if num_moves < 0:
                issues.append(f"Negative move count: {num_moves}")
            elif num_moves > 300:
                issues.append(f"Suspiciously high move count: {num_moves}")
        
        # ELO validation
        for player in ['white_elo', 'black_elo']:
            if player in game_data and game_data[player]:
                try:
                    elo = int(game_data[player])
                    if elo < 0 or elo > 4000:
                        issues.append(f"Invalid {player}: {elo}")
                except (ValueError, TypeError):
                    issues.append(f"Non-numeric {player}: {game_data[player]}")
        
        # Update stats
        if issues:
            self.stats['invalid'] += 1
            for issue in issues:
                issue_type = issue.split(':')[0]
                self.stats['errors_by_type'][issue_type] = \
                    self.stats['errors_by_type'].get(issue_type, 0) + 1
        else:
            self.stats['valid'] += 1
        
        return len(issues) == 0, issues
    
    def get_quality_score(self) -> float:
        """Calculate overall data quality score (0-100)"""
        if self.stats['total_checked'] == 0:
            return 100.0
        return (self.stats['valid'] / self.stats['total_checked']) * 100
    
    def get_report(self) -> Dict:
        """Get comprehensive quality report"""
        return {
            'total_checked': self.stats['total_checked'],
            'valid': self.stats['valid'],
            'invalid': self.stats['invalid'],
            'quality_score': round(self.get_quality_score(), 2),
            'error_breakdown': self.stats['errors_by_type']
        }
    
    def print_report(self):
        """Print formatted quality report"""
        report = self.get_report()
        print(f"\n{'='*60}")
        print(f"Data Quality Report")
        print(f"{'='*60}")
        print(f"Total Games Checked: {report['total_checked']:,}")
        print(f"Valid: {report['valid']:,}")
        print(f"Invalid: {report['invalid']:,}")
        print(f"Quality Score: {report['quality_score']}%")
        
        if report['error_breakdown']:
            print(f"\nError Breakdown:")
            for error_type, count in sorted(report['error_breakdown'].items(), 
                                           key=lambda x: x[1], reverse=True):
                print(f"  {error_type}: {count:,}")
        print(f"{'='*60}\n")
