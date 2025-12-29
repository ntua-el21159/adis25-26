import torch
from transformers import AutoTokenizer, AutoModelForCausalLM

# Using the 1.5B version so it runs fast on your laptop CPU.
# If you really want the big one (slow), change to "Qwen/Qwen2.5-Coder-7B-Instruct"
MODEL_ID = "Qwen/Qwen2.5-Coder-1.5B-Instruct"

class QwenAgent:
    def __init__(self):
        print(f"⏳ Loading {MODEL_ID} locally... (this might take a minute)")
        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        
        # Load Model
        self.tokenizer = AutoTokenizer.from_pretrained(MODEL_ID)
        self.model = AutoModelForCausalLM.from_pretrained(
            MODEL_ID,
            torch_dtype=torch.float32, # float32 is safer for CPU
            device_map=self.device
        )
        self.model.eval()
        print(f"✅ Model loaded on {self.device.upper()}")

    def generate_sql(self, schema: str, question: str) -> str:
        # Qwen-specific prompt format
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
                do_sample=False,       # Deterministic for reproducibility
                pad_token_id=self.tokenizer.eos_token_id
            )
            
        # Decode and strip prompt
        output_text = self.tokenizer.decode(generated_ids[0], skip_special_tokens=True)
        sql = output_text.replace(prompt, "").strip()
        
        # Clean up any trailing garbage (optional)
        if ";" in sql:
            sql = sql.split(";")[0] + ";"
            
        return sql