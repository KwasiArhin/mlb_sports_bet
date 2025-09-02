#!/usr/bin/env python3
"""
Prompt Pattern Generator - Advanced AI prompt engineering for code generation.

This tool implements Tree of Thought reasoning, Chain of Draft refinement,
and formal contract specification to create actionable, hallucination-resistant
prompts for AI coding models.

Usage: python prompt-pattern.py "requirements or problem description"
"""

import os
import sys
import re
import uuid
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass
import argparse
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@dataclass
class Solution:
    """Represents a solution candidate in Tree of Thought reasoning."""
    name: str
    description: str
    pros: List[str]
    cons: List[str]
    complexity: str
    yagni_score: int  # 1-10, higher = more YAGNI compliant

@dataclass
class TaskContract:
    """Formal contract specification for a coding task."""
    task_id: str
    function_name: str
    inputs: Dict[str, str]
    output: str
    preconditions: List[str]
    postconditions: List[str]
    invariants: List[str]
    error_handling: List[str]
    performance: str
    thread_safety: str
    test_cases: List[str]
    dependencies: List[str]
    additional_requirements: List[str]

class PromptPatternGenerator:
    """Main class for generating structured AI prompts with formal contracts."""
    
    def __init__(self, tasks_dir: str = "./tasks"):
        self.tasks_dir = Path(tasks_dir)
        self.tasks_dir.mkdir(exist_ok=True)
        self.task_counter = self._get_next_task_number()
    
    def _get_next_task_number(self) -> int:
        """Get the next available task number."""
        existing_tasks = list(self.tasks_dir.glob("*.md"))
        if not existing_tasks:
            return 1
        
        max_num = 0
        for task_file in existing_tasks:
            match = re.match(r"(\d+)-", task_file.name)
            if match:
                max_num = max(max_num, int(match.group(1)))
        return max_num + 1
    
    def analyze_requirements(self, requirements: str) -> Dict[str, Any]:
        """
        Deep analysis of requirements to extract key components.
        
        Args:
            requirements: Raw requirement description from user
            
        Returns:
            Dictionary with analyzed components
        """
        logger.info("🧠 Analyzing requirements...")
        
        # Extract key elements
        analysis = {
            "core_problem": self._extract_core_problem(requirements),
            "technical_domain": self._identify_technical_domain(requirements),
            "complexity_indicators": self._assess_complexity(requirements),
            "potential_functions": self._identify_functions(requirements),
            "data_types": self._identify_data_types(requirements),
            "error_scenarios": self._identify_error_scenarios(requirements)
        }
        
        logger.info(f"Core problem: {analysis['core_problem']}")
        logger.info(f"Technical domain: {analysis['technical_domain']}")
        
        return analysis
    
    def _extract_core_problem(self, requirements: str) -> str:
        """Extract the core problem from requirements."""
        # Simple extraction - can be enhanced with NLP
        sentences = requirements.split('.')
        return sentences[0].strip() if sentences else requirements[:100]
    
    def _identify_technical_domain(self, requirements: str) -> str:
        """Identify the technical domain based on keywords."""
        domains = {
            "web_scraping": ["html", "scrape", "crawl", "web", "parse", "extract"],
            "data_processing": ["data", "process", "transform", "analyze", "csv", "json"],
            "machine_learning": ["model", "train", "predict", "classify", "ml", "ai"],
            "finance_betting": ["kelly", "criterion", "bet", "sizing", "odds", "probability", "bankroll", "wager"],
            "api": ["api", "endpoint", "request", "response", "http", "rest"],
            "database": ["database", "sql", "query", "store", "retrieve", "db"],
            "file_processing": ["file", "read", "write", "parse", "format"]
        }
        
        req_lower = requirements.lower()
        
        # Check domains in order of specificity (most specific first)
        domain_order = ["finance_betting", "machine_learning", "web_scraping", "data_processing", 
                       "api", "database", "file_processing"]
        
        for domain in domain_order:
            keywords = domains[domain]
            if any(keyword in req_lower for keyword in keywords):
                return domain
        return "general"
    
    def _assess_complexity(self, requirements: str) -> List[str]:
        """Assess complexity indicators in requirements."""
        indicators = []
        req_lower = requirements.lower()
        
        complexity_markers = {
            "high": ["concurrent", "distributed", "scale", "performance", "optimize", "async"],
            "medium": ["class", "multiple", "integrate", "handle", "manage"],
            "low": ["simple", "basic", "straightforward", "single"]
        }
        
        for level, markers in complexity_markers.items():
            if any(marker in req_lower for marker in markers):
                indicators.append(level)
        
        return indicators if indicators else ["medium"]
    
    def _identify_functions(self, requirements: str) -> List[str]:
        """Identify potential function names from requirements."""
        # Extract verbs and convert to function names
        common_verbs = ["extract", "parse", "process", "analyze", "generate", "create", 
                       "validate", "transform", "fetch", "save", "load", "classify"]
        
        req_lower = requirements.lower()
        functions = []
        
        # Domain-specific function patterns
        if "kelly" in req_lower and "criterion" in req_lower:
            functions.append("calculate_kelly_bet_size")
        elif "bet" in req_lower and "size" in req_lower:
            functions.append("calculate_bet_size")
        elif "extract" in req_lower and "article" in req_lower:
            functions.append("extract_article_text")
        elif "classify" in req_lower and "content" in req_lower:
            functions.append("classify_content")
        else:
            for verb in common_verbs:
                if verb in req_lower:
                    functions.append(f"{verb}_content")
        
        if not functions:
            functions.append("process_data")
        
        return functions[:3]  # Limit to 3 main functions
    
    def _identify_data_types(self, requirements: str) -> Dict[str, str]:
        """Identify input/output data types from requirements."""
        type_mapping = {
            "html": "str", "text": "str", "string": "str", "url": "str",
            "json": "Dict[str, Any]", "dict": "Dict[str, Any]", "object": "Dict[str, Any]",
            "list": "List[Any]", "array": "List[Any]", "items": "List[Any]",
            "number": "float", "integer": "int", "boolean": "bool",
            "file": "Path", "path": "Path",
            "probability": "float", "odds": "float", "bankroll": "float",
            "prediction": "Dict[str, Any]", "bet": "float"
        }
        
        req_lower = requirements.lower()
        identified_types = {}
        
        # Domain-specific type inference
        if "kelly" in req_lower or "bet" in req_lower:
            identified_types.update({
                "win_probability": "float",
                "odds": "float", 
                "bankroll": "float"
            })
        elif "extract" in req_lower and "html" in req_lower:
            identified_types.update({
                "html_content": "str",
                "css_selector": "str"
            })
        
        for keyword, dtype in type_mapping.items():
            if keyword in req_lower:
                identified_types[keyword] = dtype
        
        return identified_types if identified_types else {"input": "str", "output": "str"}
    
    def _identify_error_scenarios(self, requirements: str) -> List[str]:
        """Identify potential error scenarios."""
        scenarios = [
            "Invalid input format",
            "Network connectivity issues",
            "Resource not found",
            "Processing timeout",
            "Memory limitations"
        ]
        
        req_lower = requirements.lower()
        relevant_scenarios = []
        
        if any(word in req_lower for word in ["network", "url", "api", "fetch"]):
            relevant_scenarios.extend(["Network timeout", "HTTP errors", "Invalid URLs"])
        
        if any(word in req_lower for word in ["file", "read", "write"]):
            relevant_scenarios.extend(["File not found", "Permission denied", "Disk full"])
        
        if any(word in req_lower for word in ["parse", "json", "xml", "html"]):
            relevant_scenarios.extend(["Malformed data", "Encoding issues"])
        
        return relevant_scenarios[:5]  # Limit to top 5
    
    def generate_solutions_tree_of_thought(self, analysis: Dict[str, Any]) -> List[Solution]:
        """
        Generate 3 solution alternatives using Tree of Thought reasoning.
        
        Args:
            analysis: Requirements analysis from analyze_requirements()
            
        Returns:
            List of 3 solution candidates
        """
        logger.info("🌳 Generating Tree of Thought solutions...")
        
        core_problem = analysis["core_problem"]
        domain = analysis["technical_domain"]
        complexity = analysis["complexity_indicators"]
        
        solutions = []
        
        # Solution A: Simple, direct approach
        solution_a = Solution(
            name="Direct Implementation",
            description=f"Straightforward solution focusing on core functionality for {core_problem}",
            pros=[
                "Quick to implement and understand",
                "Low complexity and maintenance",
                "Minimal dependencies",
                "Easy to test and debug"
            ],
            cons=[
                "May lack flexibility for edge cases",
                "Limited scalability",
                "Basic error handling"
            ],
            complexity="O(n)",
            yagni_score=9
        )
        solutions.append(solution_a)
        
        # Solution B: Robust, production-ready approach
        solution_b = Solution(
            name="Production-Ready Implementation",
            description=f"Comprehensive solution with full error handling and monitoring for {core_problem}",
            pros=[
                "Comprehensive error handling",
                "Performance monitoring",
                "Extensible architecture",
                "Production-ready features"
            ],
            cons=[
                "Higher complexity",
                "More dependencies",
                "Longer development time",
                "Potential over-engineering"
            ],
            complexity="O(n log n)",
            yagni_score=6
        )
        solutions.append(solution_b)
        
        # Solution C: Domain-specific optimized approach
        solution_c = Solution(
            name=f"{domain.title()}-Optimized Implementation",
            description=f"Domain-specific solution optimized for {domain} requirements",
            pros=[
                f"Optimized for {domain} use cases",
                "Leverages domain-specific libraries",
                "Better performance for target use case",
                "Industry best practices"
            ],
            cons=[
                "Less generalizable",
                "Domain-specific dependencies",
                "May be complex for simple cases"
            ],
            complexity="O(1)" if "simple" in complexity else "O(n)",
            yagni_score=7
        )
        solutions.append(solution_c)
        
        return solutions
    
    def select_best_solution(self, solutions: List[Solution]) -> Solution:
        """
        Select the best solution based on YAGNI score and requirements fit.
        
        Args:
            solutions: List of solution candidates
            
        Returns:
            Best solution candidate
        """
        logger.info("🎯 Selecting best solution...")
        
        # Sort by YAGNI score (higher is better)
        best_solution = max(solutions, key=lambda s: s.yagni_score)
        
        logger.info(f"Selected: {best_solution.name} (YAGNI score: {best_solution.yagni_score})")
        
        return best_solution
    
    def chain_of_draft_refinement(self, solution: Solution, analysis: Dict[str, Any]) -> TaskContract:
        """
        Refine solution using Chain of Draft with critical self-review.
        
        Args:
            solution: Selected solution from Tree of Thought
            analysis: Requirements analysis
            
        Returns:
            Refined task contract
        """
        logger.info("📝 Starting Chain of Draft refinement...")
        
        # Draft 1: Basic contract
        draft1 = self._create_basic_contract(solution, analysis)
        logger.info("Draft 1: Basic contract created")
        
        # Review Draft 1
        draft1_issues = self._review_contract(draft1)
        logger.info(f"Draft 1 review found {len(draft1_issues)} issues")
        
        # Draft 2: Address issues
        draft2 = self._refine_contract(draft1, draft1_issues)
        logger.info("Draft 2: Issues addressed")
        
        # Final review
        final_issues = self._review_contract(draft2)
        if final_issues:
            logger.warning(f"Final draft still has {len(final_issues)} minor issues")
        
        return draft2
    
    def _create_basic_contract(self, solution: Solution, analysis: Dict[str, Any]) -> TaskContract:
        """Create basic contract from solution and analysis."""
        functions = analysis["potential_functions"]
        data_types = analysis["data_types"]
        error_scenarios = analysis["error_scenarios"]
        domain = analysis["technical_domain"]
        
        main_function = functions[0] if functions else "process_data"
        
        # Domain-specific contract creation
        if domain == "finance_betting" and "kelly" in main_function:
            inputs = {
                "win_probability": "float",
                "decimal_odds": "float",
                "bankroll": "float",
                "max_bet_fraction": "float = 0.25"
            }
            output = "float"
            preconditions = [
                "0.0 < win_probability < 1.0",
                "decimal_odds > 1.0",
                "bankroll > 0.0",
                "0.0 < max_bet_fraction <= 1.0"
            ]
            postconditions = [
                "0.0 <= result <= bankroll * max_bet_fraction",
                "Result is 0.0 when Kelly criterion is negative",
                "Result respects maximum bet fraction limit"
            ]
            test_cases = [
                "calculate_kelly_bet_size(0.6, 2.0, 1000.0) returns positive bet size",
                "calculate_kelly_bet_size(0.4, 2.0, 1000.0) returns 0.0 (negative edge)",
                "calculate_kelly_bet_size(0.8, 1.5, 1000.0) respects max_bet_fraction"
            ]
            dependencies = ["numpy>=1.21.0"]
        else:
            # Generic contract fallback
            inputs = {
                "input_data": list(data_types.values())[0] if data_types else "str",
                "config": "Optional[Dict[str, Any]]"
            }
            output = list(data_types.values())[-1] if len(data_types) > 1 else "Any"
            preconditions = [
                "input_data is not None",
                "input_data meets format requirements"
            ]
            postconditions = [
                "Result meets specified format",
                "No side effects on input data",
                "Returns within timeout limits"
            ]
            test_cases = [
                "Valid input returns expected output",
                "Invalid input raises appropriate exception",
                "Edge cases are handled gracefully"
            ]
            dependencies = []
        
        return TaskContract(
            task_id=f"TASK-{self.task_counter:03d}",
            function_name=main_function,
            inputs=inputs,
            output=output,
            preconditions=preconditions,
            postconditions=postconditions,
            invariants=[
                "Function is pure and stateless",
                "Memory usage is bounded"
            ],
            error_handling=error_scenarios[:3],
            performance=solution.complexity,
            thread_safety="Function is thread-safe",
            test_cases=test_cases,
            dependencies=dependencies,
            additional_requirements=[]
        )
    
    def _review_contract(self, contract: TaskContract) -> List[str]:
        """Critical review of contract to find issues."""
        issues = []
        
        # Check for vague specifications
        if "Any" in contract.output:
            issues.append("Output type is too generic")
        
        if len(contract.preconditions) < 2:
            issues.append("Insufficient preconditions specified")
        
        if len(contract.error_handling) < 3:
            issues.append("Need more specific error handling scenarios")
        
        if not contract.test_cases or len(contract.test_cases) < 3:
            issues.append("Insufficient test cases for verification")
        
        # Check for over-engineering indicators
        if len(contract.preconditions) > 8:
            issues.append("Potential over-engineering in preconditions")
        
        return issues
    
    def _refine_contract(self, contract: TaskContract, issues: List[str]) -> TaskContract:
        """Refine contract based on identified issues."""
        refined = contract
        
        # Address generic output type
        if "Output type is too generic" in issues:
            if "extract" in contract.function_name:
                refined.output = "Optional[str]"
            elif "parse" in contract.function_name:
                refined.output = "Dict[str, Any]"
            elif "process" in contract.function_name:
                refined.output = "List[Any]"
        
        # Add more specific error handling
        if "Need more specific error handling scenarios" in issues:
            refined.error_handling.extend([
                "Timeout after 30 seconds",
                "Memory limit exceeded",
                "Invalid configuration parameters"
            ])
        
        # Add more test cases
        if "Insufficient test cases" in issues:
            refined.test_cases.extend([
                "Performance under load",
                "Memory usage stays within bounds",
                "Concurrent access safety"
            ])
        
        # Add missing preconditions
        if "Insufficient preconditions" in issues:
            refined.preconditions.extend([
                "System has sufficient memory",
                "Required dependencies are available"
            ])
        
        return refined
    
    def generate_task_file(self, contract: TaskContract, requirements: str) -> str:
        """
        Generate the final task file with formal contract specification.
        
        Args:
            contract: Refined task contract
            requirements: Original requirements
            
        Returns:
            Path to generated task file
        """
        logger.info(f"📋 Generating task file: {contract.task_id}")
        
        # Create problem slug from requirements
        problem_slug = re.sub(r'[^a-zA-Z0-9\-]', '-', requirements[:50]).strip('-').lower()
        problem_slug = re.sub(r'-+', '-', problem_slug)
        
        filename = f"{contract.task_id.split('-')[1]}-{problem_slug}.md"
        filepath = self.tasks_dir / filename
        
        # Generate file content
        content = self._generate_task_content(contract, requirements)
        
        # Write file
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(content)
        
        logger.info(f"✅ Task file created: {filepath}")
        return str(filepath)
    
    def _generate_task_content(self, contract: TaskContract, requirements: str) -> str:
        """Generate the markdown content for task file."""
        
        # Format inputs
        inputs_str = "\n".join([f"  - {name}: {dtype}" for name, dtype in contract.inputs.items()])
        
        # Format lists
        preconditions_str = "\n".join([f"  - {item}" for item in contract.preconditions])
        postconditions_str = "\n".join([f"  - {item}" for item in contract.postconditions])
        invariants_str = "\n".join([f"  - {item}" for item in contract.invariants])
        error_handling_str = "\n".join([f"  - {item}" for item in contract.error_handling])
        test_cases_str = "\n".join([f"{i+1}. {case}" for i, case in enumerate(contract.test_cases)])
        
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        content = f"""## {contract.task_id}
---
STATUS: OPEN

**Generated:** {timestamp}
**Original Requirements:** {requirements}

Implement `{contract.function_name}` with the following contract:

### Input Parameters:
{inputs_str}

### Output:
- Returns: {contract.output}

### Preconditions:
{preconditions_str}

### Postconditions:
{postconditions_str}

### Invariants:
{invariants_str}

### Error Handling:
{error_handling_str}

### Performance:
- Complexity: {contract.performance}

### Thread Safety:
- {contract.thread_safety}

### Implementation Requirements:
- Generate the implementation with comprehensive error handling
- Include docstring with examples
- Add type hints for all parameters and return values
- Follow YAGNI principles - implement only what's needed
- Use appropriate design patterns for the problem domain

### Test Cases That MUST Pass:
{test_cases_str}

### Dependencies:
{chr(10).join([f"- {dep}" for dep in contract.dependencies]) if contract.dependencies else "- No external dependencies required"}

### Additional Notes:
{chr(10).join([f"- {req}" for req in contract.additional_requirements]) if contract.additional_requirements else "- Follow standard Python best practices"}

---
**Generated by PromptPatternGenerator v1.0**
"""
        return content
    
    def process_requirements(self, requirements: str) -> str:
        """
        Main processing pipeline for requirements.
        
        Args:
            requirements: User requirements or problem description
            
        Returns:
            Path to generated task file
        """
        logger.info("🚀 Starting requirement processing pipeline...")
        
        # Step 1: Analyze requirements
        analysis = self.analyze_requirements(requirements)
        
        # Step 2: Generate solution alternatives (Tree of Thought)
        solutions = self.generate_solutions_tree_of_thought(analysis)
        
        # Step 3: Select best solution
        best_solution = self.select_best_solution(solutions)
        
        # Step 4: Refine with Chain of Draft
        contract = self.chain_of_draft_refinement(best_solution, analysis)
        
        # Step 5: Generate task file
        task_file = self.generate_task_file(contract, requirements)
        
        logger.info(f"✨ Processing complete! Task file: {task_file}")
        
        return task_file


def main():
    """Main entry point for the prompt pattern generator."""
    parser = argparse.ArgumentParser(
        description="Advanced AI prompt engineering for code generation",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python prompt-pattern.py "Extract article text from HTML"
  python prompt-pattern.py "Create a web scraper for product prices"
  python prompt-pattern.py "Implement a machine learning classifier"
        """
    )
    
    parser.add_argument(
        "requirements",
        help="Requirements or problem description to generate prompts for"
    )
    
    parser.add_argument(
        "--tasks-dir",
        default="./tasks",
        help="Directory to store generated task files (default: ./tasks)"
    )
    
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose logging"
    )
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    try:
        generator = PromptPatternGenerator(tasks_dir=args.tasks_dir)
        task_file = generator.process_requirements(args.requirements)
        
        print(f"\n🎉 SUCCESS: Task file generated at: {task_file}")
        print("\nNext steps:")
        print("1. Review the generated task specification")
        print("2. Implement the function according to the contract")
        print("3. Run the test cases to verify implementation")
        print("4. Update STATUS to DOING -> DONE when complete")
        
    except Exception as e:
        logger.error(f"❌ Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()