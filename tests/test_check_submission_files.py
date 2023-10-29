"""Verify required files are present."""

import os
import bs4
import markdown


def test_check_submission_files():
    """Check for files and directories required by the spec."""
    assert os.path.exists("bin")
    assert os.path.exists("bin/mapreduce")
    assert os.path.exists("pyproject.toml")
    assert os.path.exists("mapreduce")
    assert os.path.exists("mapreduce/manager/__init__.py")
    assert os.path.exists("mapreduce/manager/__main__.py")
    assert os.path.exists("mapreduce/worker/__init__.py")
    assert os.path.exists("mapreduce/worker/__main__.py")
    assert os.path.exists("mapreduce/__init__.py")
    assert os.path.exists("mapreduce/submit.py")
    assert (
        os.path.exists("mapreduce/utils.py") or
        os.path.exists("mapreduce/utils")
    )


def test_check_gai_survey():
    """Check for and validate GAI.md."""
    assert os.path.exists("GAI.md")

    with open("GAI.md", encoding="utf-8") as md_file:
        md_text = md_file.read()

    # Parse the HTML content
    html_text = markdown.markdown(md_text, extensions=["attr_list"])
    soup = bs4.BeautifulSoup(html_text, "html.parser")

    # Extract answers
    num_questions = 31
    answers = {}
    for i in range(1, num_questions+1):
        qid = f"Q{i:02d}"
        question = soup.find(id=qid)
        assert question, f"Failed to find {qid}"
        answer = question.find_next()
        text = answer.text.strip()
        if answer.name == "ul" and qid == "Q10":
            # Multiple selection
            answers.update(validate_muliple_selection(answer, qid))
        elif answer.name == "ul":
            # Multiple choice
            answers[qid] = validate_multiple_choice(answer, qid)
        elif answer.name == "p" and text.isdigit():
            # Numeric
            answers[qid] = int(text)
        else:
            # Text
            answers[qid] = text

    # Validate class standings
    total = 0
    for i in range(2, 7):  # Q2 - Q6 inclusive
        count = int(answers[f"Q0{i}"])
        assert 0 <= count <= 3
        total += count
    assert 0 < total <= 3

    # Assert valid free response answers
    free_reponse_qids = [
        "Q17",
        "Q18",
        "Q22",
        "Q23",
        "Q27",
        "Q28",
    ]
    for qid in free_reponse_qids:
        assert answers[qid]
        assert answers[qid].upper() != "FIXME"


def validate_multiple_choice(ele, qid):
    """Validate multiple choice response and return answer."""
    assert ele.name == "ul"

    selection = ""
    for list_item in ele.find_all("li"):
        if list_item.text.lower().startswith("[x]"):
            assert not selection, f"Q{qid}: Expected one selection '[x]'"
            selection = list_item.text.replace("[x]", "").strip()

    return selection


def validate_muliple_selection(ele, qid):
    """Validate multiple selection response and return answer."""
    assert ele.name == "ul"

    selections = {}
    for i, list_item in enumerate(ele.find_all("li"), start=1):
        sub_qid = f"{qid}.{i}"
        if list_item.text.lower().startswith("[x]"):
            selections[sub_qid] = "Yes"
        else:
            selections[sub_qid] = "No"

    return selections
