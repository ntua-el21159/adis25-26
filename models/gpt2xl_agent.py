import re
import torch
from transformers import AutoTokenizer, AutoModelForCausalLM

MODEL_ID = "openai-community/gpt2-xl"

class GPT2XLAgent:
    def __init__(self, device: str | None = None):
        self.device = device or ("cuda" if torch.cuda.is_available() else "cpu")
        self.tokenizer = AutoTokenizer.from_pretrained(MODEL_ID)
        self.model = AutoModelForCausalLM.from_pretrained(MODEL_ID).to(self.device)

        self.tokenizer.pad_token = self.tokenizer.eos_token
        self.max_ctx = getattr(self.model.config, "n_positions", 1024)  # GPT-2 = 1024

    def build_prompt(self, schema: str, question: str) -> str:
        return (
            "### Database schema:\n"
            f"{schema}\n\n"
            "### Question:\n"
            f"{question}\n\n"
            "### SQL:\n"
            "SELECT"
        )

    def _make_inputs_under_limit(self, schema: str, question: str, max_new_tokens: int):
        """
        Ensure total tokens fit: prompt + max_new_tokens <= max_ctx
        Strategy: progressively truncate schema tokens (keep question intact).
        """
        # Reserve space for generation
        budget = self.max_ctx - max_new_tokens
        if budget <= 0:
            raise ValueError(f"max_new_tokens={max_new_tokens} leaves no room for prompt in ctx={self.max_ctx}")

        # Tokenize question/prompt parts separately so we only truncate schema
        prefix = "### Database schema:\n"
        mid = "\n\n### Question:\n"
        suffix = f"{question}\n\n### SQL:\nSELECT"

        prefix_ids = self.tokenizer(prefix, add_special_tokens=False).input_ids
        mid_ids = self.tokenizer(mid, add_special_tokens=False).input_ids
        suffix_ids = self.tokenizer(suffix, add_special_tokens=False).input_ids

        fixed_len = len(prefix_ids) + len(mid_ids) + len(suffix_ids)

        # If even fixed parts exceed budget, truncate question (rare)
        if fixed_len >= budget:
            # Keep the tail of suffix (question) minimal
            # Hard truncate suffix tokens to fit
            keep = max(32, budget - (len(prefix_ids) + len(mid_ids)))
            suffix_ids = suffix_ids[-keep:]
            fixed_len = len(prefix_ids) + len(mid_ids) + len(suffix_ids)

        # Now budget remaining for schema
        schema_budget = max(0, budget - fixed_len)

        schema_ids = self.tokenizer(schema, add_special_tokens=False).input_ids
        if len(schema_ids) > schema_budget:
            schema_ids = schema_ids[:schema_budget]

        input_ids = prefix_ids + schema_ids + mid_ids + suffix_ids
        attn = [1] * len(input_ids)

        return {
            "input_ids": torch.tensor([input_ids], device=self.device),
            "attention_mask": torch.tensor([attn], device=self.device),
        }

    def generate_sql(self, schema: str, question: str, max_new_tokens: int = 160) -> str:
        inputs = self._make_inputs_under_limit(schema, question, max_new_tokens=max_new_tokens)

        with torch.no_grad():
            out = self.model.generate(
                **inputs,
                max_new_tokens=max_new_tokens,
                do_sample=False,
                pad_token_id=self.tokenizer.eos_token_id,
                eos_token_id=self.tokenizer.eos_token_id,
            )

        text = self.tokenizer.decode(out[0], skip_special_tokens=True)
        return self._extract_sql(text)

    @staticmethod
    def _extract_sql(generated_text: str) -> str:
        upper = generated_text.upper()
        start = upper.find("SELECT")
        if start == -1:
            return generated_text.strip()

        tail = generated_text[start:]

        semi = tail.find(";")
        if semi != -1:
            return tail[: semi + 1].strip()

        for sep in ["\n\n", "\r\n\r\n", "\n", "\r\n"]:
            idx = tail.find(sep)
            if idx != -1:
                return tail[:idx].strip()

        return tail.strip()
