import sqlite3
import json

conn = sqlite3.connect("data/sakura_ko.db")
cursor = conn.cursor()

TYPE_MAP = {
    "en": {1: "STR", 2: "DEX", 3: "QCK", 4: "PSY", 5: "INT"},
    "ko": {1: "힘", 2: "기", 3: "속", 4: "심", 5: "지"}
}

CLASS_MAP = {
    "en": {
        1: "Fighter", 2: "Slasher", 3: "Striker", 4: "Shooter",
        5: "Free Spirit", 6: "Driven", 7: "Cerebral", 8: "Powerhouse"
    },
    "ko": {
        1: "격투형", 2: "참격형", 3: "타격형", 4: "사격형",
        5: "자유형", 6: "야심형", 7: "박식형", 8: "강인형"
    }
}

LANG = "en"

def get_type_name(attr_id):
    return TYPE_MAP.get(LANG, {}).get(attr_id, "알수없음")

def get_classes(class1, class2):
    name1 = CLASS_MAP.get(LANG, {}).get(class1, f"Class{class1}")
    name2 = CLASS_MAP.get(LANG, {}).get(class2, f"Class{class2}")
    return name1 if class2 == -1 else [name1, name2]

def make_name(name, sub_name):
    return f"{name} – {sub_name}" if sub_name else name

cursor.execute("""
SELECT 
    logbookId_, name_, subName_, attributeId_, characterType_, subCharacterType_,
    rarity_, isRarityPlus_, cost_, comboNum_, maxOptionSkill_, 
    maxLevel_, limitExp_, minHealth_, minAttackDamage_, minRestoration_, 
    maxHealth_, maxAttackDamage_, maxRestoration_
FROM MstCharacter_
WHERE logbookId_ != -1
ORDER BY logbookId_ ASC
""")

units_by_id = {}
max_logbook_id = 0
for row in cursor.fetchall():
    (
        logbook_id, name, sub_name, attr_id, class1, class2,
        rarity, rarity_plus, cost, combo, sockets,
        max_lvl, exp_to_max, lvl1_hp, lvl1_atk, lvl1_rcv,
        max_hp, max_atk, max_rcv
    ) = row

    display_name = make_name(name, sub_name)
    type_name = get_type_name(attr_id)
    class_info = get_classes(class1, class2)
    stars = f"{rarity}+" if rarity_plus else rarity

    unit = [
        display_name,
        type_name,
        class_info,
        stars,
        cost,
        combo,
        sockets,
        max_lvl,
        exp_to_max,
        lvl1_hp,
        lvl1_atk,
        lvl1_rcv,
        max_hp,
        max_atk,
        max_rcv,
        1  # Growth Rate
    ]

    units_by_id[logbook_id] = unit  # ❗ 누락된 부분
    if logbook_id > max_logbook_id:
        max_logbook_id = logbook_id

# 빠진 logbookId를 빈 유닛으로 채움
units = []
for i in range(1, max_logbook_id + 1):
    if i in units_by_id:
        units.append(units_by_id[i])
    else:
        units.append([
            "", "Type", ["Class1", "Class2"],
            None, None, None, None, None, None,
            None, None, None, None, None, None, None
        ])

with open("./data/units.js", "w", encoding="utf-8") as f:
    f.write("window.units = [\n")
    for unit in units:
        f.write("  " + json.dumps(unit, ensure_ascii=False) + ",\n")
    f.write("];\n")

print("✅ units.js 파일 생성 완료 (logbookId 순 정렬됨)")
