import torch
from transformers import AutoTokenizer, AutoModelForCausalLM

model_id = "Abdelkareem/supra-50m-arabic-sft-mix"

print("جاري تحميل النموذج...")
tokenizer = AutoTokenizer.from_pretrained(model_id)
model = AutoModelForCausalLM.from_pretrained(model_id)
model.eval()

titles = [
    "توريد وتركيب خوادم وشبكات حاسب آلي",
    "استئجار سيارات لمدة ثلاث سنوات",
    "تقديم خدمات دعم للهيئة",
]

for title in titles:
    messages = [{
        "role": "user",
        "content": (
            "صنف المناقصة إلى: تقنية، غير تقنية، أو تحتاج مراجعة.\n"
            "التقنية تشمل البرمجيات والحاسب والشبكات والأمن السيبراني.\n"
            "إذا العنوان غير واضح اختر تحتاج مراجعة.\n"
            "اكتب التصنيف وسببًا قصيرًا فقط.\n"
            f"عنوان المناقصة: {title}"
        ),
    }]

    prompt = tokenizer.apply_chat_template(
        messages,
        tokenize=False,
        add_generation_prompt=True,
    )

    inputs = tokenizer(
        prompt,
        return_tensors="pt",
        add_special_tokens=False,
    )

    with torch.inference_mode():
        output = model.generate(
            **inputs,
            max_new_tokens=80,
            do_sample=False,
            pad_token_id=tokenizer.eos_token_id,
        )

    answer = tokenizer.decode(
        output[0, inputs["input_ids"].shape[1]:],
        skip_special_tokens=True,
    )

    print("\nالمناقصة:", title)
    print("تصنيف النموذج:", answer)
    print("-" * 50)