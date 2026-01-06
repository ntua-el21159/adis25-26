import torch
import re
from transformers import AutoTokenizer, AutoModelForCausalLM

# Χρησιμοποιούμε την έκδοση 1.5B για να τρέχει γρήγορα στο laptop σου
MODEL_ID = "Qwen/Qwen2.5-Coder-1.5B-Instruct"

class QwenAgent:
    def __init__(self):
        print(f"⏳ Loading {MODEL_ID} locally... (this might take a minute)")
        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        
        # Φόρτωση του Μοντέλου
        self.tokenizer = AutoTokenizer.from_pretrained(MODEL_ID)
        self.model = AutoModelForCausalLM.from_pretrained(
            MODEL_ID,
            torch_dtype=torch.float32, 
            device_map=self.device
        )
        self.model.eval()
        print(f"✅ Model loaded on {self.device.upper()}")

    def generate_sql(self, schema: str, question: str) -> str:
        prompt = (
            f"### Database schema:\n{schema}\n\n"
            f"### Question:\n{question}\n\n"
            f"### SQL:\n"
        )
        
        inputs = self.tokenizer(prompt, return_tensors="pt").to(self.device)
        
        with torch.no_grad():
            generated_ids = self.model.generate(
                **inputs,
                max_new_tokens=256,
                do_sample=False,
                pad_token_id=self.tokenizer.eos_token_id
            )
            
        output_text = self.tokenizer.decode(generated_ids[0], skip_special_tokens=True)
        
        # --- ΤΕΛΙΚΟΣ ΚΑΘΑΡΙΣΜΟΣ (FINAL CLEANING) ---
        
        # 1. Κρατάμε μόνο το κείμενο μετά το Prompt
        if "### SQL:" in output_text:
            raw_answer = output_text.split("### SQL:")[-1].strip()
        else:
            raw_answer = output_text.replace(prompt, "").strip()

        # 2. Ελέγχουμε αν υπάρχει code block (```sql ... ```)
        # Αυτό το regex ψάχνει κείμενο ανάμεσα στα backticks
        code_block_match = re.search(r"```(?:sql)?\s*(.*?)\s*```", raw_answer, re.DOTALL | re.IGNORECASE)
        
        if code_block_match:
            # Αν βρήκαμε code block, παίρνουμε ΜΟΝΟ το περιεχόμενό του
            sql = code_block_match.group(1).strip()
        else:
            # Αν δεν υπάρχουν backticks, παίρνουμε όλο το κείμενο
            sql = raw_answer

        # 3. ΤΟ ΣΗΜΑΝΤΙΚΟΤΕΡΟ: Κόβουμε τα πάντα μετά το πρώτο ερωτηματικό (;)
        # Το SQL τελειώνει πάντα με ;. Οτιδήποτε μετά είναι "μπλα-μπλα" του μοντέλου.
        if ";" in sql:
            sql = sql.split(";")[0] + ";"
            
        # 4. Τελευταίο καθάρισμα για τυχόν σκουπίδια που έμειναν
        sql = sql.replace("```", "").strip()
        
        return sql