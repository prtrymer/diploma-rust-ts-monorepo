import { i as __toESM, n as require_react, t as styled_default } from "./dist-7mcv8Z4W.js";
import { a as isInnerOnlyCell, i as isEditableGridCell, n as ThemeContext, o as isObjectEditorCallbackResult, r as makeCSSStyle, s as require_react_dom, t as ClickOutsideContainer } from "./index-BavvU2QC.js";
//#region node_modules/@glideapps/glide-data-grid/dist/esm/internal/data-grid-overlay-editor/data-grid-overlay-editor-style.js
var import_react = /* @__PURE__ */ __toESM(require_react(), 1);
var import_react_dom = /* @__PURE__ */ __toESM(require_react_dom(), 1);
var _exp2 = () => (p) => p.targetX;
var _exp3 = () => (p) => p.targetY;
var _exp4 = () => (p) => p.targetWidth;
var _exp5 = () => (p) => p.targetHeight;
var _exp6 = () => (p) => p.targetY + 10;
var _exp7 = () => (p) => Math.max(0, (p.targetHeight - 28) / 2);
var DataGridOverlayEditorStyle = /* @__PURE__ */ styled_default("div")({
	name: "DataGridOverlayEditorStyle",
	class: "gdg-d19meir1",
	propsAsIs: false,
	vars: {
		"d19meir1-0": [_exp3(), "px"],
		"d19meir1-1": [_exp2(), "px"],
		"d19meir1-2": [_exp4(), "px"],
		"d19meir1-3": [_exp5(), "px"],
		"d19meir1-4": [_exp6(), "px"],
		"d19meir1-5": [_exp7(), "px"]
	}
});
//#endregion
//#region node_modules/@glideapps/glide-data-grid/dist/esm/internal/data-grid-overlay-editor/use-stay-on-screen.js
function useRefState() {
	const [refState, setRefState] = import_react.useState();
	return [refState ?? void 0, setRefState];
}
function useStayOnScreen() {
	const [ref, setRef] = useRefState();
	const [xOffset, setXOffset] = import_react.useState(0);
	const [isIntersecting, setIsIntersecting] = import_react.useState(true);
	import_react.useLayoutEffect(() => {
		if (ref === void 0) return;
		if (!("IntersectionObserver" in window)) return;
		const observer = new IntersectionObserver((ents) => {
			if (ents.length === 0) return;
			setIsIntersecting(ents[0].isIntersecting);
		}, { threshold: 1 });
		observer.observe(ref);
		return () => observer.disconnect();
	}, [ref]);
	import_react.useEffect(() => {
		if (isIntersecting || ref === void 0) return;
		let rafHandle;
		const fn = () => {
			const { right: refRight } = ref.getBoundingClientRect();
			setXOffset((cv) => Math.min(cv + window.innerWidth - refRight - 10, 0));
			rafHandle = requestAnimationFrame(fn);
		};
		rafHandle = requestAnimationFrame(fn);
		return () => {
			if (rafHandle !== void 0) cancelAnimationFrame(rafHandle);
		};
	}, [ref, isIntersecting]);
	return {
		ref: setRef,
		style: import_react.useMemo(() => {
			return { transform: `translateX(${xOffset}px)` };
		}, [xOffset])
	};
}
//#endregion
//#region node_modules/@glideapps/glide-data-grid/dist/esm/internal/data-grid-overlay-editor/data-grid-overlay-editor.js
var DataGridOverlayEditor = (p) => {
	const { target, content, onFinishEditing: onFinishEditingIn, forceEditMode, initialValue, imageEditorOverride, markdownDivCreateNode, highlight, className, theme, id, cell, bloom, validateCell, getCellRenderer, provideEditor, isOutsideClick } = p;
	const [tempValue, setTempValueRaw] = import_react.useState(forceEditMode ? content : void 0);
	const lastValueRef = import_react.useRef(tempValue ?? content);
	lastValueRef.current = tempValue ?? content;
	const [isValid, setIsValid] = import_react.useState(() => {
		if (validateCell === void 0) return true;
		return !(isEditableGridCell(content) && validateCell?.(cell, content, lastValueRef.current) === false);
	});
	const onFinishEditing = import_react.useCallback((newCell, movement) => {
		onFinishEditingIn(isValid ? newCell : void 0, movement);
	}, [isValid, onFinishEditingIn]);
	const setTempValue = import_react.useCallback((newVal) => {
		if (validateCell !== void 0 && newVal !== void 0 && isEditableGridCell(newVal)) {
			const validResult = validateCell(cell, newVal, lastValueRef.current);
			if (validResult === false) setIsValid(false);
			else if (typeof validResult === "object") {
				newVal = validResult;
				setIsValid(true);
			} else setIsValid(true);
		}
		setTempValueRaw(newVal);
	}, [cell, validateCell]);
	const finished = import_react.useRef(false);
	const customMotion = import_react.useRef(void 0);
	const onClickOutside = import_react.useCallback(() => {
		onFinishEditing(tempValue, [0, 0]);
		finished.current = true;
	}, [tempValue, onFinishEditing]);
	const onEditorFinished = import_react.useCallback((newValue, movement) => {
		onFinishEditing(newValue, movement ?? customMotion.current ?? [0, 0]);
		finished.current = true;
	}, [onFinishEditing]);
	const onKeyDown = import_react.useCallback(async (event) => {
		let save = false;
		if (event.key === "Escape") {
			event.stopPropagation();
			event.preventDefault();
			customMotion.current = [0, 0];
		} else if (event.key === "Enter" && !event.shiftKey) {
			event.stopPropagation();
			event.preventDefault();
			customMotion.current = [0, 1];
			save = true;
		} else if (event.key === "Tab") {
			event.stopPropagation();
			event.preventDefault();
			customMotion.current = [event.shiftKey ? -1 : 1, 0];
			save = true;
		}
		window.setTimeout(() => {
			if (!finished.current && customMotion.current !== void 0) {
				onFinishEditing(save ? tempValue : void 0, customMotion.current);
				finished.current = true;
			}
		}, 0);
	}, [onFinishEditing, tempValue]);
	const targetValue = tempValue ?? content;
	const [editorProvider, useLabel] = import_react.useMemo(() => {
		if (isInnerOnlyCell(content)) return [];
		const external = provideEditor?.(content);
		if (external !== void 0) return [external, false];
		return [getCellRenderer(content)?.provideEditor?.(content), false];
	}, [
		content,
		getCellRenderer,
		provideEditor
	]);
	const { ref, style: stayOnScreenStyle } = useStayOnScreen();
	let pad = true;
	let editor;
	let style = true;
	let styleOverride;
	if (editorProvider !== void 0) {
		pad = editorProvider.disablePadding !== true;
		style = editorProvider.disableStyling !== true;
		const isObjectEditor = isObjectEditorCallbackResult(editorProvider);
		if (isObjectEditor) styleOverride = editorProvider.styleOverride;
		const CustomEditor = isObjectEditor ? editorProvider.editor : editorProvider;
		editor = import_react.createElement(CustomEditor, {
			isHighlighted: highlight,
			onChange: setTempValue,
			value: targetValue,
			initialValue,
			onFinishedEditing: onEditorFinished,
			validatedSelection: isEditableGridCell(targetValue) ? targetValue.selectionRange : void 0,
			forceEditMode,
			target,
			imageEditorOverride,
			markdownDivCreateNode,
			isValid,
			theme
		});
	}
	styleOverride = {
		...styleOverride,
		...stayOnScreenStyle
	};
	const portalElement = document.getElementById("portal");
	if (portalElement === null) {
		console.error("Cannot open Data Grid overlay editor, because portal not found.  Please add `<div id=\"portal\" />` as the last child of your `<body>`.");
		return null;
	}
	let classWrap = style ? "gdg-style" : "gdg-unstyle";
	if (!isValid) classWrap += " gdg-invalid";
	if (pad) classWrap += " gdg-pad";
	const bloomX = bloom?.[0] ?? 1;
	const bloomY = bloom?.[1] ?? 1;
	return (0, import_react_dom.createPortal)(import_react.createElement(ThemeContext.Provider, { value: theme }, import_react.createElement(ClickOutsideContainer, {
		style: makeCSSStyle(theme),
		className,
		onClickOutside,
		isOutsideClick
	}, import_react.createElement(DataGridOverlayEditorStyle, {
		ref,
		id,
		className: classWrap,
		style: styleOverride,
		as: useLabel === true ? "label" : void 0,
		targetX: target.x - bloomX,
		targetY: target.y - bloomY,
		targetWidth: target.width + bloomX * 2,
		targetHeight: target.height + bloomY * 2
	}, import_react.createElement("div", {
		className: "gdg-clip-region",
		onKeyDown
	}, editor)))), portalElement);
};
//#endregion
export { DataGridOverlayEditor as default };
