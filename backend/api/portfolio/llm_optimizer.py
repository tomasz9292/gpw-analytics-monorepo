from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional, Sequence


@dataclass
class OptimizationStep:
    iteration: int
    weights: Dict[str, float]
    score: float
    top_symbols: List[str]
    llm_response: Optional[str] = None


@dataclass
class OptimizationResult:
    best_weights: Dict[str, float]
    best_score: float
    steps: List[OptimizationStep]


@dataclass
class OptimizationRequest:
    feature_names: Sequence[str]
    initial_weights: Dict[str, float]
    iterations: int


class LocalLLMOptimizer:
    """Lightweight wrapper around llama.cpp compatible models."""

    def __init__(
        self,
        model_path: str,
        temperature: float = 0.1,
        max_tokens: int = 256,
        gpu_layers: Optional[int] = None,
        system_prompt: Optional[str] = None,
    ) -> None:
        try:
            from llama_cpp import Llama
        except ImportError as exc:  # pragma: no cover - optional dependency
            raise RuntimeError(
                "llama-cpp-python is required to use LocalLLMOptimizer. "
                "Install it via pip and ensure that the model fits in GPU memory."
            ) from exc

        self._model = Llama(
            model_path=model_path,
            n_gpu_layers=gpu_layers or 0,
            n_ctx=4096,
            logits_all=False,
            embedding=False,
        )
        self.temperature = temperature
        self.max_tokens = max_tokens
        self.system_prompt = system_prompt or (
            "You are an expert quantitative analyst. "
            "Propose numerical weights (summing to 1.0) for ranking features to maximise portfolio return."
        )

    def _build_prompt(
        self,
        feature_names: Sequence[str],
        history: List[OptimizationStep],
    ) -> str:
        history_lines: List[str] = []
        for step in history:
            history_lines.append(
                f"Iteration {step.iteration}: weights={json.dumps(step.weights)} score={step.score:.6f}"
            )
        history_text = "\n".join(history_lines) if history_lines else "(no previous attempts)"
        features_text = ", ".join(feature_names)
        prompt = (
            f"{self.system_prompt}\n\n"
            f"Available features: {features_text}.\n"
            f"History:\n{history_text}\n\n"
            "Respond with a JSON object mapping each feature name to a numeric weight."
        )
        return prompt

    def _parse_weights(self, raw_response: str, feature_names: Sequence[str]) -> Dict[str, float]:
        start = raw_response.find("{")
        end = raw_response.rfind("}")
        if start == -1 or end == -1 or end <= start:
            raise ValueError("LLM response does not contain a JSON object")
        snippet = raw_response[start : end + 1]
        data = json.loads(snippet)
        weights: Dict[str, float] = {}
        for name in feature_names:
            value = float(data.get(name, 0.0))
            weights[name] = max(value, 0.0)
        total = sum(weights.values())
        if total <= 0:
            raise ValueError("LLM suggested zero weights for all features")
        return {name: value / total for name, value in weights.items()}

    def optimize(
        self,
        request: OptimizationRequest,
        evaluate: Callable[[Dict[str, float]], float],
        summarise: Callable[[Dict[str, float]], List[str]],
    ) -> OptimizationResult:
        history: List[OptimizationStep] = []
        best_weights = dict(request.initial_weights)
        best_score = evaluate(best_weights)
        history.append(
            OptimizationStep(iteration=0, weights=best_weights, score=best_score, top_symbols=summarise(best_weights))
        )

        current_weights = dict(best_weights)
        for iteration in range(1, request.iterations + 1):
            prompt = self._build_prompt(request.feature_names, history)
            completion = self._model.create_completion(
                prompt=prompt,
                temperature=self.temperature,
                max_tokens=self.max_tokens,
                stop=["\n\n"],
            )
            text = completion["choices"][0]["text"].strip()
            try:
                weights = self._parse_weights(text, request.feature_names)
            except Exception as exc:  # pragma: no cover - defensive against malformed responses
                step = OptimizationStep(
                    iteration=iteration,
                    weights=current_weights,
                    score=best_score,
                    top_symbols=summarise(current_weights),
                    llm_response=text,
                )
                history.append(step)
                continue

            current_weights = weights
            score = evaluate(weights)
            top_symbols = summarise(weights)
            step = OptimizationStep(
                iteration=iteration,
                weights=weights,
                score=score,
                top_symbols=top_symbols,
                llm_response=text,
            )
            history.append(step)
            if score > best_score:
                best_score = score
                best_weights = weights

        return OptimizationResult(best_weights=best_weights, best_score=best_score, steps=history)
