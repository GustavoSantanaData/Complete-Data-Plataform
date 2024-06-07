#!/usr/bin/env bash

# Inicia o Uvicorn em segundo plano
uvicorn main:app --reload &> uvicorn.log &

wait -n
