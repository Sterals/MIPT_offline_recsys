import json

import numpy as np
import pandas as pd
import triton_python_backend_utils as pb_utils
from catboost import CatBoostClassifier


FEATURE_COLS = [
    "als_score", "als_rank",
    "age", "income", "sex", "kids_flg",
    "content_type", "release_year", "age_rating", "for_kids",
    "n_genres", "n_countries", "n_actors",
]

CAT_FEATURES = ["age", "income", "sex", "content_type", "for_kids"]


class TritonPythonModel:
    def initialize(self, args):
        model_dir = args["model_repository"] + "/1"
        self.model = CatBoostClassifier()
        self.model.load_model(f"{model_dir}/model.cbm")

    def execute(self, requests):
        responses = []
        for request in requests:
            # Входные данные — массив JSON-строк, каждая строка = dict фичей одного кандидата
            input_tensor = pb_utils.get_input_tensor_by_name(request, "features")
            json_strings = input_tensor.as_numpy().astype(str).tolist()

            rows = [json.loads(s) for s in json_strings]
            df = pd.DataFrame(rows)[FEATURE_COLS]

            # Предикт
            scores = self.model.predict_proba(df)[:, 1].astype(np.float32)

            out_tensor = pb_utils.Tensor("scores", scores)
            responses.append(pb_utils.InferenceResponse([out_tensor]))

        return responses
